from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(F.countDistinct("units").alias("unique_stores"))
    .filter(F.col("unique_stores") >= 4)
)
