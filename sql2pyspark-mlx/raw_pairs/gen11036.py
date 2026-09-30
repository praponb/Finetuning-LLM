from pyspark.sql import functions as F

product_summary = (
    df_order_items
    .filter(F.col("product_id") == 27)
    .groupBy("product_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_products.alias("products")
    .join(product_summary.alias("product_summary"), F.col("products.id") == F.col("product_summary.product_id"), "inner")
    .filter(F.col("product_summary.min_quantity") >= 5.7)
    .select(
        F.col("products.id"),
        F.col("products.brand"),
        F.col("products.cost"),
        F.col("product_summary.unique_orders"),
        F.col("product_summary.min_quantity"),
    )
    .orderBy(F.col("id").desc())
)
