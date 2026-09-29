from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.filter(F.col("brand") == "Stark").alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left_semi")
    .filter(F.col("store_sales.store_id").isin([1, 2, 5, 6]))
    .select(F.col("store_sales.id"), F.col("store_sales.store_id"))
    .orderBy(F.col("id").desc())
)
