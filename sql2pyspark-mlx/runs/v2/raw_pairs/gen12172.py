from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("order_id").isin([2, 11, 12, 40]))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "inner")
    .filter(F.col("product_totals.avg_quantity") >= 68)
    .select(
        F.col("products.id"),
        F.col("products.launch_date"),
        F.col("products.product_name"),
        F.col("product_totals.avg_quantity"),
        F.col("product_totals.total_unit_price"),
    )
    .orderBy("id", "avg_quantity")
)
