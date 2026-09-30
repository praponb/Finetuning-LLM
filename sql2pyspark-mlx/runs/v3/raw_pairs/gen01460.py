from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .unionByName(df_store_sales.select(F.col("revenue").alias("cost")))
)
