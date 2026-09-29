from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("order_id").isin([18, 28]))
    .groupBy("product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.unit_price_range") > 55)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("ps.unit_price_range"),
        F.col("ps.unique_products"),
    )
)
