from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("price").alias("value"))
    .unionByName(df_store_sales.select(F.col("revenue").alias("value")))
    .distinct()
)
