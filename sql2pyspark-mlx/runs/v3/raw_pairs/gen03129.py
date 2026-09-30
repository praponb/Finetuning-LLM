from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.max("units").alias("max_units"),
        F.count("*").alias("row_count"),
    )
)
