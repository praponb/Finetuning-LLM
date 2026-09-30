from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.sqft") >= 13119, "high").when(F.col("s.sqft") >= 3953, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("o.channel") == "store", F.col("o.amount")).otherwise(0)).alias("store_amount"),
        F.sum(F.col("o.shipping_fee")).alias("total_shipping_fee"),
    )
)
