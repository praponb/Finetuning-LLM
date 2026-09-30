from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(
        F.count("*").alias("row_count"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)
