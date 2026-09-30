from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4]))
    .groupBy("store_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy("units_range")
)
