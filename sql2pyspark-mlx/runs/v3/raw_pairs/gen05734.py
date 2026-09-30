from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 5]))
    .groupBy("product_id")
    .agg(
        F.min("units").alias("min_units"),
        F.count("units").alias("units_count"),
        F.max("units").alias("max_units"),
    )
    .filter(F.col("units_count") >= 5)
    .orderBy(F.col("units_count").desc())
)
