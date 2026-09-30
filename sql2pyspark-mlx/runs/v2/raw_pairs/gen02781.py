from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left")
    .filter(F.month(F.col("store_sales.sale_date")).isin([2, 7, 9]))
    .select(
        F.col("products.category"),
        F.col("products.stock_qty"),
        F.col("products.price"),
        F.col("store_sales.units"),
        F.col("store_sales.sale_date"),
    )
)
