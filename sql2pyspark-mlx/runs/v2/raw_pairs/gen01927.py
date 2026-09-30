from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.product_name").like("%Hub%"))
    .groupBy(
        F.col("p.category"),
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"),
    )
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
    )
    .filter(F.col("unit_price_count") > 1)
    .orderBy(F.col("max_unit_price").desc(), "category", "launch_month")
    .limit(5)
)
