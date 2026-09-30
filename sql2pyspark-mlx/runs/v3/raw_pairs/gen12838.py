from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.avg_salary") >= 43)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("t.avg_salary"),
        F.col("t.director_count"),
    )
)
