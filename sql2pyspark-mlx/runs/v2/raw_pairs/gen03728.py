from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNull())
    .groupBy("product_id")
    .agg(F.countDistinct("units").alias("unique_stores"))
    .orderBy(F.col("unique_stores").desc(), "product_id")
)
