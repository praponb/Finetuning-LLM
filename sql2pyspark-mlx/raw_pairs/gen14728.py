from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") >= 2025)
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .filter(F.col("units_range") >= 47)
    .orderBy("units_range")
)
