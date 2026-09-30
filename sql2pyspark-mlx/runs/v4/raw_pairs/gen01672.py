from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.discount"))
)
