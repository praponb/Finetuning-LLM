from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 5]) & (F.col("units") <= 15))
    .select("sale_date", (F.col("units") + F.col("revenue")).alias("revenue_per_unit"))
    .orderBy(F.col("revenue_per_unit").desc())
)
