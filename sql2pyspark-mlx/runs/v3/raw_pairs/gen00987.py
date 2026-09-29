from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.max(F.col("e.salary")).alias("max_salary"),
        F.sum(F.when(F.col("e.job_title") == "Director", F.col("e.salary")).otherwise(0)).alias("director_salary"),
    )
    .filter(F.col("director_salary") > 50)
    .orderBy(
        F.col("director_salary").desc(),
        F.col("employee_count").desc(),
        "id",
        "dept_name",
    )
    .limit(10)
)
