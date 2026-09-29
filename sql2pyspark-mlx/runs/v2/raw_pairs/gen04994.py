from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_summary.alias("department_summary"), F.col("departments.id") == F.col("department_summary.dept_id"), "left")
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("department_summary.designer_salary"),
        F.col("department_summary.job_title_values"),
    )
)
