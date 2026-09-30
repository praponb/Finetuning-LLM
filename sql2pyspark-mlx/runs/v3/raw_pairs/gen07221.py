from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("t.min_unit_price"),
        F.col("t.unit_price_count"),
    )
    .orderBy("brand")
)
