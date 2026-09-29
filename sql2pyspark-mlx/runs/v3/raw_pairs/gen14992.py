from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("*").alias("total_count"),
        (F.max("units") - F.min("units")).alias("units_range"),
        F.sum("units").alias("total_units"),
    )
)
