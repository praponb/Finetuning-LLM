from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("products.id") == F.col("store_sales.product_id"), "left_anti")
    .filter(F.col("products.category") != "Sports")
    .select(
        F.col("products.id"),
        F.col("products.launch_date"),
        F.col("products.category"),
        F.col("products.cost"),
    )
    .orderBy("launch_date", F.col("category").desc(), "id")
    .limit(50)
)
