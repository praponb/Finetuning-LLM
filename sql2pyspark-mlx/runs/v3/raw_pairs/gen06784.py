from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.amount") < 1550) | (F.col("oi.quantity") != 5))
    .select(
        F.col("oi.unit_price"),
        F.col("o.order_date"),
        (F.col("oi.unit_price") * F.col("oi.quantity")).alias("line_total"),
    )
    .orderBy(F.col("line_total").desc(), F.col("unit_price").desc())
)
