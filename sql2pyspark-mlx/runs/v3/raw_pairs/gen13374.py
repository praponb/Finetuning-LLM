from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 420)
    .select(F.col("price").alias("value"))
    .unionByName(
        df_store_sales
        .filter(F.col("store_id") == 2)
        .select(F.col("revenue").alias("value"))
    )
    .distinct()
)
