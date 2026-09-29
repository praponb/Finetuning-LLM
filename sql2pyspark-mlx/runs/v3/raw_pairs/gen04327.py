from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 3, 9]) & (F.col("revenue") > 1900))
    .groupBy("product_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .filter(F.col("units_range") > 20)
)
