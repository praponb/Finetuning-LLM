from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.countDistinct("units").alias("unique_stores"),
        F.max("units").alias("max_units"),
    )
)
