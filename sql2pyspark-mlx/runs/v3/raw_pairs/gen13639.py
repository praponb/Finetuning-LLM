from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 5, 6]) & (F.col("product_id").isin([7, 8, 10]) | (F.col("units") <= 16)))
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("units_range").desc())
)
