from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2025-07-22")
    .groupBy("store_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .orderBy("sale_month")
)
