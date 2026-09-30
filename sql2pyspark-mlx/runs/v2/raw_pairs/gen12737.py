from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.filter(F.col("cost") < 700).alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left_anti")
    .filter(F.col("store_sales.revenue") < 2100)
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.store_id"),
        F.col("store_sales.units"),
    )
    .orderBy(F.col("store_id").desc())
)
