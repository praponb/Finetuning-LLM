from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0))) / F.count("*")), 1).alias("pct_director"),
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
    )
    .filter(F.col("engineer_count") >= 30)
)
