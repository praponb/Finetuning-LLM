from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("dt.director_count"),
    )
    .orderBy(F.col("director_count").desc(), "id")
    .limit(3)
)
