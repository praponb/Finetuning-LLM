from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") >= 2025)
    .groupBy("store_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .filter(F.col("units_range") > 48)
    .select("store_id", "units_range")
    .orderBy(F.col("store_id").desc(), F.col("units_range").desc())
)
