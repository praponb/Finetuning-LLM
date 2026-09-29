from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
    )
)
