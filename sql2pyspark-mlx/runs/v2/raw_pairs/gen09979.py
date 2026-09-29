from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.count("*").alias("cnt"),
    )
)
