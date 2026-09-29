from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", "level", "is_remote")
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
    )
)
