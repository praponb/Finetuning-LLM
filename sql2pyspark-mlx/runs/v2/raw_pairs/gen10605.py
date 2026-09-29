from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.store_name").like("%Centra%") & F.col("o.amount").between(530, 1270))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.concat_ws("-", F.col("o.status"), F.col("o.channel")).alias("status_channel_key"),
    )
)
