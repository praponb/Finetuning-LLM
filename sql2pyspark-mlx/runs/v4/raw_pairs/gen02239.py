from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.channel"), F.col("o.status"), F.col("o.discount"))
)
