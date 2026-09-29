from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 5]))
    .select("id", "units", F.date_sub("sale_date", 61).alias("sale_date_minus_61d"))
)
