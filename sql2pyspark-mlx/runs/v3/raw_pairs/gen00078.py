from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "Central")
    .select("id", "store_name")
    .orderBy("store_name")
)
