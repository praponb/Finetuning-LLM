from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4, 5]))
    .groupBy("product_id")
    .agg(
        F.countDistinct("units").alias("unique_stores"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.max("units").alias("max_units"),
    )
)
