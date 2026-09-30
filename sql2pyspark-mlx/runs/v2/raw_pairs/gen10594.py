from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5]) & F.col("product_id").isin([4, 10]))
    .select("id", "revenue", "units", F.round("units", 1).alias("units_rounded"))
)
