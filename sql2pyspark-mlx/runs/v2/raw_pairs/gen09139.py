from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.dept_name"), F.col("d.location"))
    .agg(
        F.sum(F.when(F.col("e.job_title") == "Director", F.col("e.salary")).otherwise(0)).alias("director_salary"),
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
    )
    .orderBy(F.col("dept_name").desc())
)
