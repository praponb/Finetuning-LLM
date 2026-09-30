from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "East", "Central"]))
    .select("region")
    .orderBy(F.col("region").desc())
)
