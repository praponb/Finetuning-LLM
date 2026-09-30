from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("o.status"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("s.store_name"),
        F.concat_ws("-", F.col("o.status"), F.col("o.channel")).alias("status_channel_key"),
    )
)
