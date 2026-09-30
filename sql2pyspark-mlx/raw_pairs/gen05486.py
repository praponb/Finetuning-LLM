from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "East")
    .select("city", F.upper("store_name").alias("store_name_upper"))
)
