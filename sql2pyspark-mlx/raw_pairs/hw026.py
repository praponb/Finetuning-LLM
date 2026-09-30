from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", "product_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.avg("revenue").alias("avg_revenue"),
    )
)
