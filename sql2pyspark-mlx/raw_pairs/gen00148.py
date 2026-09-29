from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(16, 29) & F.col("revenue").isNull())
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
    .filter(F.col("total_revenue") >= 7000)
    .orderBy(F.col("product_id").desc())
)
