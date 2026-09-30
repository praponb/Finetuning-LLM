from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
    )
    .orderBy("level")
)
