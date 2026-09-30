from pyspark.sql import functions as F

recent_employees = df_employees.filter(F.col("salary") <= 78000)

summary = (
    recent_employees
    .groupBy(
        F.when(F.col("salary") >= 162000, "high").when(F.col("salary") >= 69000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.max("salary").alias("max_salary"),
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
    )
)

result = (
    summary
    .filter(F.col("max_salary") < 128000)
    .orderBy(F.col("manager_count").desc())
)
