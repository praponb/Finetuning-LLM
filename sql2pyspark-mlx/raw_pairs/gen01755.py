from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~((F.col("revenue") >= 1600) & F.col("store_id").isin([2, 5, 6]) & F.col("product_id").isin([1, 7, 8, 10])))
    .select("id", "store_id", "product_id", F.round("units", 2).alias("units_rounded"))
)
