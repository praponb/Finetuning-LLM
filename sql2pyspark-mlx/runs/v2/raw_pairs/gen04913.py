from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 7, 8, 10]) & (F.col("revenue") <= 1500))
    .groupBy("product_id")
    .agg(
        F.max("units").alias("max_units"),
        F.avg("units").alias("avg_units"),
    )
    .filter(F.col("avg_units") > 13)
)
