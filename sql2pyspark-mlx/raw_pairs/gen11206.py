from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("order_id").isin([13, 21, 29]))
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("product_totals"), F.col("products.id") == F.col("product_totals.product_id"), "inner")
    .filter(F.col("product_totals.unique_products") > 6)
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("products.stock_qty"),
        F.col("product_totals.unique_products"),
    )
    .orderBy(F.col("stock_qty").desc(), "category")
)
