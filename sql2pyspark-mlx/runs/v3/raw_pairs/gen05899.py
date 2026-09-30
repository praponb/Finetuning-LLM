from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "South")
    .select("id", "store_name")
)
