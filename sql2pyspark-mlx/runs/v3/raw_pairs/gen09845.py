from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 5) & F.col("store_id").isin([1, 2, 3, 6]))
    .select("revenue", "store_id", "units")
)
