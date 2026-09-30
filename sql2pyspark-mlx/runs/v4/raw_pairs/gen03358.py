from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") != "South")
    .select("region")
    .intersect(df_stores.filter(F.col("region") == "West").select("region"))
)
