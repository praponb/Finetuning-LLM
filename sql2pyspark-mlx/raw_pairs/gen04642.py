from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg((F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"))
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.unit_price_range") >= 58)
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("pt.unit_price_range"),
    )
    .orderBy("unit_price_range")
)
