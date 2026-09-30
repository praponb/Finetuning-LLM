from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "West") | F.col("region").isin(["South", "Central", "West"]))
    .select("id", "region", "city")
    .orderBy("city", F.col("id").desc())
)
