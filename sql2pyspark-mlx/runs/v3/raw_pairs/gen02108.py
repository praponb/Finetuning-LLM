from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.count("units").alias("units_count"),
    )
)
