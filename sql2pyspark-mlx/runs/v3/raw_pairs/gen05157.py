from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.min("sale_date").alias("min_sale_date"))
    .orderBy(F.col("min_sale_date").desc(), F.col("product_id").desc())
    .limit(3)
)
