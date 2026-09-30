from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 4]) & (F.col("revenue") < 1100))
    .select("store_id", "sale_date", F.ceil("revenue").alias("revenue_whole"))
    .orderBy(F.col("store_id").desc())
)
