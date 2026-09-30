from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.max("units").alias("max_units"),
    )
    .filter(F.col("avg_units") >= 18)
    .orderBy(F.col("product_id").desc())
)
