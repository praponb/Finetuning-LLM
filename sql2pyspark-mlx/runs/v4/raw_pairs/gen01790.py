from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2025-09-01")
    .groupBy("store_id")
    .agg(F.sum("units").alias("total_units"))
    .filter(F.col("total_units") > 90)
    .select("store_id", "total_units")
    .orderBy(F.col("store_id").desc(), "total_units")
)
