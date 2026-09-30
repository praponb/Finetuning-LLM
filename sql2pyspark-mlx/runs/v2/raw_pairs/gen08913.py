from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
        F.count("*").alias("total_count"),
    )
)
