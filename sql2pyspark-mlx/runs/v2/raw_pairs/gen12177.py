from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "West", "South"]) & (~F.col("region").isin(["South", "Central"]) | (F.col("region") == "South")))
    .select("id", "city", "store_name", "sqft")
)
