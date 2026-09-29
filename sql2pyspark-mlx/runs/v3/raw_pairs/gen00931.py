from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_store_sales.filter(F.col("revenue") >= 3400).alias("store_sales"), F.col("stores.id") == F.col("store_sales.store_id"), "left_semi")
    .filter(F.col("stores.sqft") != 7228)
    .select(F.col("stores.id"), F.col("stores.city"))
)
