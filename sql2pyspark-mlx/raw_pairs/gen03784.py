from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Chen%"))
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
    )
    .orderBy("hire_month")
    .limit(10)
)
