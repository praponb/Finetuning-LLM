from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .groupBy("product_id")
    .agg(F.avg("units").alias("avg_units"))
    .orderBy(F.col("product_id").desc())
)
