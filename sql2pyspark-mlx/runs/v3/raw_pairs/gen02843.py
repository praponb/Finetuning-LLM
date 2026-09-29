from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.category").isin(["Home", "Fashion"]) & (F.col("products.supplier_id") == 6))
    .select(
        F.col("products.product_name"),
        F.col("products.launch_date"),
        F.col("products.stock_qty"),
        F.col("store_sales.units"),
        F.col("store_sales.revenue").cast("int").alias("revenue_int"),
    )
    .orderBy("revenue_int")
)
