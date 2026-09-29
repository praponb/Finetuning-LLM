from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "outer")
    .filter(F.col("products.supplier_id") == 10)
    .select(
        F.col("store_sales.product_id"),
        F.col("store_sales.units"),
        F.col("products.price"),
        F.col("products.cost"),
        F.col("products.stock_qty"),
    )
)
