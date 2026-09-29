from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "outer")
    .groupBy(
        F.col("products.category"),
        F.month(F.col("products.launch_date")).alias("launch_month"),
    )
    .agg(F.countDistinct(F.col("store_sales.units")).alias("unique_stores"))
    .orderBy("launch_month", "unique_stores", "category")
    .limit(20)
)
