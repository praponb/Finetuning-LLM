from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.discount") >= 0.3)
    .select(
        F.col("s.region"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.concat_ws(",", F.col("o.status"), F.col("o.channel")).alias("status_channel_key"),
    )
)
