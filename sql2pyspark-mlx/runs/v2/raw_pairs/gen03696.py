from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("job_title") == "Director")
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.avg_salary") > 46)
    .select(F.col("d.location"), F.col("dt.avg_salary"), F.col("dt.salary_range"))
)
