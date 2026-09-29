from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("salary").alias("min_salary"),
        F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0)).alias("director_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "left")
    .select(
        F.col("d.budget"),
        F.col("d.location"),
        F.col("E.min_salary"),
        F.col("E.director_count"),
    )
)
