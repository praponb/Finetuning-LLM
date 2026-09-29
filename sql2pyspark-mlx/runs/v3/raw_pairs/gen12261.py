from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .filter(F.col("units_range") >= 66)
    .orderBy(F.col("product_id").desc())
    .limit(3)
)
