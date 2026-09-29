from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") == "Tokyo") & (F.col("segment") == "Corporate") & F.col("segment").isin(["SMB", "Corporate", "Consumer"]))
    .groupBy("is_active")
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
        F.countDistinct("credit_limit").alias("unique_segments"),
    )
)
