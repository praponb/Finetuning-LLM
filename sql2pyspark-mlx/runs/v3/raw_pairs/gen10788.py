from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.max_unit_price") >= 110)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("t.max_unit_price"),
    )
)
