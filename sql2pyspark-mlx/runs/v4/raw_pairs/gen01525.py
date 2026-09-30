from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(23, 33) & (F.col("product_id") == 8))
    .groupBy("store_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.max("units").alias("max_units"),
    )
    .orderBy(F.col("max_units").desc(), F.col("store_id").desc())
)
