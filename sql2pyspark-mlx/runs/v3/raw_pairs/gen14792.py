from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.sum("unit_price").alias("total_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.total_unit_price") > 2200)
    .select(F.col("o.channel"), F.col("o.customer_id"), F.col("os.total_unit_price"))
    .orderBy(F.col("total_unit_price").desc(), "channel")
)
