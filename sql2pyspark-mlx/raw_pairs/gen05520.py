from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("level") == "L3")
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "left")
    .filter(F.col("department_totals.total_salary") > 660000)
    .select(
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("department_totals.total_salary"),
        F.col("department_totals.job_title_values"),
    )
)
