from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.min("price").alias("min_price"))
    .filter(F.col("min_price") >= 950)
    .orderBy(F.col("min_price").desc(), F.col("brand").desc())
)
