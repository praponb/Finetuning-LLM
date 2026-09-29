from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("hire_date") < "2024-06-01")
    .withColumn("clean_level", F.trim("level"))
)

employees_stats = (
    valid_employees
    .groupBy("job_title")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = employees_stats.orderBy(F.col("avg_salary").desc(), "job_title")
