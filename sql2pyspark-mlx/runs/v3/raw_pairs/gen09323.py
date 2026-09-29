from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([7, 8, 9]) & ((F.col("revenue") > 3400) | (F.col("revenue") >= 2000)))
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .orderBy(F.col("units_range").desc())
)
