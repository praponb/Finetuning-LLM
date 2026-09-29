from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "job_title")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 5)
    .orderBy("job_title")
)
