from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("min_revenue") < 1600)
)
