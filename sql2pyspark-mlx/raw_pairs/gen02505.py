from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
    )
    .orderBy("latest_hire_date")
)
