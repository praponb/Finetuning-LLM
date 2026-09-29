from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.count("units").alias("units_count"),
    )
    .filter(F.col("units_count") >= 8)
)
