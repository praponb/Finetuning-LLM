from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 5, 6]) & (F.col("units") > 39) & (F.col("revenue") > 2700))
    .select("store_id", "revenue", F.quarter("sale_date").alias("sale_date_quarter"))
    .orderBy(F.col("store_id").desc())
)
