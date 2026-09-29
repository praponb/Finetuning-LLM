from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 9)
    .groupBy("store_id")
    .agg(F.countDistinct("units").alias("unique_stores"))
    .filter(F.col("unique_stores") > 8)
    .orderBy("unique_stores")
)
