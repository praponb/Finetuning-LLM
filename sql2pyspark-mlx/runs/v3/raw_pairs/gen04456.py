from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(~F.col("p.brand").isin(["Acme", "Initech", "Umbrella"]))
    .groupBy(F.col("p.id"), F.col("p.category"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
    )
    .orderBy(F.col("id").desc(), "order_item_count")
)
