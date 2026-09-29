from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id").isin([4, 6, 7]) & F.col("product_id").isin([9, 10])) | F.col("product_id").isin([4, 8]))
    .groupBy("product_id")
    .agg(
        F.count("units").alias("units_count"),
        F.count("revenue").alias("revenue_count"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)
