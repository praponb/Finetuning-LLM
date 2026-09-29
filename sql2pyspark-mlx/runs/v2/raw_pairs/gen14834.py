from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "store", "web"]) | (F.col("shipping_fee") > 26))
    .select(
        "id",
        "customer_id",
        "store_id",
        F.round((F.col("shipping_fee") * 0.85), 2).alias("shipping_fee_adjusted"),
        F.concat_ws("/", "status", "channel").alias("status_channel_key"),
    )
    .orderBy("id", F.col("customer_id").desc())
)
