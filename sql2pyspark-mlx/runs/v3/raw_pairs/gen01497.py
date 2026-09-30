from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5, 6]))
    .select("id", "store_id", "units")
)
