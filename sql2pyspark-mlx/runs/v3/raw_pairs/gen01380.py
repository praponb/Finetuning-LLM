from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 6, 9, 10]) & (F.col("units") >= 35))
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
    .orderBy(F.col("product_id").desc())
)
