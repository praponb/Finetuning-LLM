from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.cost") < 480)
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"), F.col("p.brand"))
    .agg(
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
        F.max(F.col("oi.quantity")).alias("max_quantity"),
    )
    .filter(F.col("unit_price_range") >= 65)
    .orderBy("max_quantity")
)
