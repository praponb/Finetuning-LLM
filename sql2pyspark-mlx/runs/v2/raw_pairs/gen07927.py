from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4]))
    .select("sale_date", (F.col("revenue") + F.col("units")).alias("revenue_per_unit"))
)
