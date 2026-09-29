from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["East", "West", "South"]))
    .select("region", "city")
    .distinct()
)
