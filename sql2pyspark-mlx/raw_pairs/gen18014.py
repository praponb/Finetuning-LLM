from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.max("units").alias("max_units"),
    )
)
