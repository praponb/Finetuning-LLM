from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .groupBy("level", "job_title")
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
    )
    .orderBy("job_title")
)
