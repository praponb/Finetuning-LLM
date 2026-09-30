from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.min("revenue").alias("min_revenue"))
    .orderBy(F.col("min_revenue").desc(), "product_id")
    .limit(5)
)
