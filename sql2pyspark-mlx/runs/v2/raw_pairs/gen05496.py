from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("product_id") == 33)
    .groupBy("order_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.revenue") >= 51)
    .select(F.col("o.discount"), F.col("t.revenue"))
    .orderBy("discount", F.col("revenue").desc())
)
