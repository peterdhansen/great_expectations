import ast
import importlib
import json
import logging
import os
import sys
import traceback
from subprocess import CalledProcessError, CompletedProcess, run

logger = logging.getLogger(__name__)
chandler = logging.StreamHandler(stream=sys.stdout)
chandler.setLevel(logging.DEBUG)
chandler.setFormatter(
    logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%dT%H:%M:%S")
)
logger.addHandler(chandler)
logger.setLevel(logging.DEBUG)


def execute_shell_command(command: str) -> int:
    """
    Execute a shell (bash in the present case) command from inside Python program.

    While developed independently, this function is very similar to the one, offered in this StackOverflow article:
    https://stackoverflow.com/questions/30993411/environment-variables-using-subprocess-check-output-python

    :param command: bash command -- as if typed in a shell/Terminal window
    :return: status code -- 0 if successful; all other values (1 is the most common) indicate an error
    """
    cwd: str = os.getcwd()

    path_env_var: str = os.pathsep.join([os.environ.get("PATH", os.defpath), cwd])
    env: dict = dict(os.environ, PATH=path_env_var)

    status_code: int = 0
    try:
        res: CompletedProcess = run(
            args=["bash", "-c", command],
            stdin=None,
            input=None,
            stdout=None,
            stderr=None,
            capture_output=True,
            shell=False,
            cwd=cwd,
            timeout=None,
            check=True,
            encoding=None,
            errors=None,
            text=None,
            env=env,
            universal_newlines=True,
        )
        sh_out: str = res.stdout.strip()
        logger.info(sh_out)
    except CalledProcessError as cpe:
        status_code = cpe.returncode
        sys.stderr.write(cpe.output)
        sys.stderr.flush()
        exception_message: str = "A Sub-Process call Exception occurred.\n"
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(cpe).__name__}: "{str(cpe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)

    return status_code


def get_contrib_requirements(filepath):
    with open(filepath) as file:
        tree = ast.parse(file.read())

    requirements_info = {"classes": [], "requirements": []}
    for child in ast.iter_child_nodes(tree):
        if not isinstance(child, ast.ClassDef):
            continue
        requirements_info["classes"] += child.name
        for node in ast.walk(child):
            if isinstance(node, ast.Assign):
                try:
                    target_ids = [target.id for target in node.targets]
                except (ValueError, AttributeError):
                    # some assignment types assign to non-node objects (e.g. Tuple)
                    target_ids = []
                if "library_metadata" in target_ids:
                    library_metadata = ast.literal_eval(node.value)
                    requirements_info["requirements"] += library_metadata.get(
                        "requirements", []
                    )

    return requirements_info


def build_gallery(include_core=True, include_contrib_experimental=True):
    logger.info("Getting base registered expectations list")
    import great_expectations

    core_expectations = (
        great_expectations.expectations.registry.list_registered_expectation_implementations()
    )

    if include_contrib_experimental:
        logger.info("Finding contrib modules")
        contrib_experimental_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "contrib",
            "experimental",
            "great_expectations_experimental",
        )
        sys.path.append(contrib_experimental_dir)
        expectations_module = importlib.import_module(
            "expectations", "great_expectations_experimental"
        )
        requirements_dict = {}
        for root, dirs, files in os.walk(contrib_experimental_dir):
            for file in files:
                if file.endswith(".py") and not file == "__init__.py":
                    logger.debug(f"Getting requirements for module {file}")
                    requirements_dict[file[:-3]] = get_contrib_requirements(
                        os.path.join(root, file)
                    )

        for expectation_module in expectations_module.__all__:
            if expectation_module in requirements_dict:
                logger.info(f"Loading dependencies for module {expectation_module}")
                for req in requirements_dict[expectation_module]["requirements"]:
                    logger.debug(f"Executing command: 'pip install \"{req}\"'")
                    execute_shell_command(f'pip install "{req}"')
            logger.debug(f"Importing {expectation_module}")
            importlib.import_module(
                f"expectations.{expectation_module}", "great_expectations_experimental"
            )
        metrics_module = importlib.import_module(
            "metrics", "great_expectations_experimental"
        )
        for metrics_module in metrics_module.__all__:
            if metrics_module in requirements_dict:
                logger.info(f"Loading dependencies for module {metrics_module}")
                for req in requirements_dict[metrics_module]["requirements"]:
                    logger.debug(f"Executing command: 'pip install \"{req}\"'")
                    execute_shell_command(f'pip install "{req}"')
            logger.debug(f"Importing {metrics_module}")
            importlib.import_module(
                f"metrics.{metrics_module}", "great_expectations_experimental"
            )

    # Above imports may have added additional expectations from contrib
    all_expectations = (
        great_expectations.expectations.registry.list_registered_expectation_implementations()
    )

    if include_core:
        build_expectations = set(all_expectations)
    else:
        build_expectations = set(all_expectations) - set(core_expectations)

    logger.info(
        f"Preparing to build gallery metadata for expectations: {build_expectations}"
    )
    gallery_info = dict()
    for expectation in build_expectations:
        logger.debug(f"Running diagnostics for expectation: {expectation}")
        impl = great_expectations.expectations.registry.get_expectation_impl(
            expectation
        )
        diagnostics = impl().run_diagnostics()
        gallery_info[expectation] = diagnostics

    return gallery_info


if __name__ == "__main__":
    gallery_info = build_gallery(include_core=True, include_contrib_experimental=True)
    with open("./expectation_library.json", "w") as outfile:
        json.dump(gallery_info, outfile)
