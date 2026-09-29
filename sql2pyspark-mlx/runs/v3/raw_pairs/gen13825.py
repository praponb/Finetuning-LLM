from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") != 39) & F.col("store_id").isin([2, 5]))
    .groupBy("product_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .orderBy(F.col("units_range").desc())
)
