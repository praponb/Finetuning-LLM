from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "right")
    .filter(F.col("store_sales.product_id").isin([2, 4, 6, 10]))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("store_sales.units"),
        F.col("store_sales.product_id"),
        F.col("products.price"),
        F.col("products.launch_date"),
        F.col("products.brand"),
    )
)
