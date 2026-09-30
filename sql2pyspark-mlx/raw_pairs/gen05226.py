from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("device").alias("distinct_devices"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
