from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("o.status") != "CANCELLED")
    .groupBy(
        F.col("s.region"),
        F.when(F.col("s.sqft") >= 16598, "high").when(F.col("s.sqft") >= 4976, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("o.channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc(), "sqft_bucket", "region")
    .limit(10)
)
