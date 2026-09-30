from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("product_id") == 9)
    .groupBy("product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "inner")
    .filter(F.col("product_totals.avg_quantity") >= 62)
    .select(
        F.col("products.id"),
        F.col("products.launch_date"),
        F.col("product_totals.avg_quantity"),
    )
    .orderBy("launch_date")
)
