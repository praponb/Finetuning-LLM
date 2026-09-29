from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "West", "Central"]))
    .select("region")
    .distinct()
)
