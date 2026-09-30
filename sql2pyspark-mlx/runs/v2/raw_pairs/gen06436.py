from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Director"]))
    .groupBy("dept_id")
    .agg(
        F.max("salary").alias("max_salary"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("t.max_salary"),
        F.col("t.last_hire_date"),
    )
)
