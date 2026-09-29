from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.price") < 1010)
    .groupBy(F.col("o.channel"))
    .agg(
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
    )
    .orderBy("unit_price_range")
)
