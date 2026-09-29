from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["North", "West", "Central"]))
    .select("region")
    .unionByName(df_stores.filter(F.col("sqft") <= 12555).select("region"))
    .distinct()
    .orderBy(F.col("region").desc())
)
