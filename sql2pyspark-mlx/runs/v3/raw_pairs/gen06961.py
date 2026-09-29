from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .filter(F.col("units_range") > 51)
    .orderBy(F.col("units_range").desc())
)
