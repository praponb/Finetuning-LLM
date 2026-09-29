from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1400, 2800))
    .groupBy("store_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.avg("units").alias("avg_units"))
    .orderBy("sale_month", F.col("store_id").desc())
)
