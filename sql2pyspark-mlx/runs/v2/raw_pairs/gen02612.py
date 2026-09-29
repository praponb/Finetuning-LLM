from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .groupBy("product_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("product_totals.revenue"),
    )
    .orderBy(F.col("product_name").desc())
)
