from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("product_name").isNotNull())
    .select(
        "id",
        "product_name",
        F.least("price", "stock_qty").alias("least_price_stock_qty"),
        F.substring("category", 1, 2).alias("category_prefix"),
    )
)
