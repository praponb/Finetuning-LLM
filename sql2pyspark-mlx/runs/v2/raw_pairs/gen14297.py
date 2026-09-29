from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("product_id") == 12).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .filter(F.col("o.store_id").isNotNull())
    .select(F.col("o.id"), F.col("o.status"), F.col("o.order_date"), F.col("o.channel"))
)
