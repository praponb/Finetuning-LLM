from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 1160)
    .select(
        "price",
        F.least("stock_qty", "price").alias("least_stock_qty_price"),
        F.lower("product_name").alias("product_name_lower"),
    )
)
