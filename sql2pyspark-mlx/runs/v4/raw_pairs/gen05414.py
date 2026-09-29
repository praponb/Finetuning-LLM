from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4]))
    .select("id", "store_id")
    .unionByName(df_store_sales.filter(F.col("units") <= 27).select("id", "store_id"))
    .distinct()
)
