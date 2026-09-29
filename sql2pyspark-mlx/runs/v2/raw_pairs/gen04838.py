from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("product_id").desc())
)
