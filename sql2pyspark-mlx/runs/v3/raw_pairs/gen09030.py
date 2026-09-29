from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left")
    .filter(F.col("products.cost").isNotNull())
    .select(
        F.col("products.brand"),
        F.col("products.launch_date"),
        F.col("store_sales.product_id"),
        F.col("store_sales.store_id"),
        (F.col("store_sales.revenue") - F.col("store_sales.units")).alias("revenue_per_unit"),
    )
)
