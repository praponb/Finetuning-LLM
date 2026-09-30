from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "West")
    .select("region")
    .unionByName(df_stores.filter(F.col("city") != "Singapore").select("region"))
)
