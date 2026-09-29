from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") < "2023-10-01") | F.col("store_id").isin([2, 5]))
    .groupBy("product_id")
    .agg(F.countDistinct("revenue").alias("unique_products"))
    .orderBy(F.col("product_id").desc())
    .limit(3)
)
