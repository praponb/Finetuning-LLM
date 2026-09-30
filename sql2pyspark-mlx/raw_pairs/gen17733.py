from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("store_id").isin([2, 4, 5]) & (F.col("units") == 20)) | (F.col("sale_date") <= "2025-07-01"))
)

agg = (
    base
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.count("*").alias("store_sale_count"))
)

result = (
    agg
    .filter(F.col("store_sale_count") > 6)
    .select("sale_month", "store_sale_count")
    .orderBy(F.col("sale_month").desc())
)
