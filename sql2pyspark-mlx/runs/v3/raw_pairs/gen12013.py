from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
    )
    .filter(F.col("total_quantity") > 17)
    .orderBy(F.col("unit_price_range").desc())
)
