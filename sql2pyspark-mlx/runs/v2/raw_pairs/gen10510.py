from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("product_id").isin([7, 26, 30]))
    .groupBy("order_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("ot.max_quantity"),
        F.col("ot.unit_price_range"),
    )
)
