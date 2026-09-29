from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.director_salary") >= 30)
    .select(
        F.col("departments.location"),
        F.col("departments.dept_name"),
        F.col("t.total_salary"),
        F.col("t.director_salary"),
    )
    .orderBy("location", "dept_name")
)
