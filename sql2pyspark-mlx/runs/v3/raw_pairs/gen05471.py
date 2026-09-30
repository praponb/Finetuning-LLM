from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") == "web") & F.col("store_id").isNull())
    .groupBy("customer_id")
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
    )
)
