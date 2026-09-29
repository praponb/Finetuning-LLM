from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2023-07-01")
    .groupBy("store_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .filter(F.col("units_range") < 63)
    .select("store_id", "units_range")
    .orderBy("store_id")
)
