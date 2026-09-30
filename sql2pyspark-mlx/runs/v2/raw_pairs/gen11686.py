from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("quantity") != 6)
    .groupBy("product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.count("*").alias("order_item_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "left")
    .select(
        F.col("p.product_name"),
        F.col("ps.unit_price_range"),
        F.col("ps.order_item_count"),
    )
    .orderBy("product_name", F.col("unit_price_range").desc())
)
