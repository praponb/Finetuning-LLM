from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "RETURNED")
    .groupBy(
        "channel",
        "store_id",
        F.when(F.col("shipping_fee") >= 40, "high").when(F.col("shipping_fee") >= 12, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.count("shipping_fee").alias("shipping_fee_count"))
    .filter(F.col("shipping_fee_count") >= 5)
    .orderBy(F.col("store_id").desc())
)
