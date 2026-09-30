from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("ProductSummary"), F.col("products.id") == F.col("ProductSummary.product_id"), "inner")
    .filter(F.col("ProductSummary.avg_unit_price") >= 29)
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("ProductSummary.avg_unit_price"),
        F.col("ProductSummary.avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), "avg_unit_price")
)
