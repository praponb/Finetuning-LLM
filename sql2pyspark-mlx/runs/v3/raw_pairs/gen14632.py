from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_summary.alias("department_summary"), F.col("departments.id") == F.col("department_summary.dept_id"), "left")
    .filter(F.col("department_summary.salary_range") > 28)
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
        F.col("department_summary.salary_range"),
        F.col("department_summary.level_list"),
    )
)
