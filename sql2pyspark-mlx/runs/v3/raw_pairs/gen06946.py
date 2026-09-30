from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(F.count("*").alias("store_sale_count"))
    .filter(F.col("store_sale_count") > 5)
    .orderBy(F.col("store_sale_count").desc(), "product_id", "store_id")
    .limit(1)
)
