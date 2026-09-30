from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        F.min("units").alias("min_units"),
        F.min("revenue").alias("min_revenue"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .orderBy("min_units")
)
