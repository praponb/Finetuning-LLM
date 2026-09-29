from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "Central"]) & (F.col("sqft") != 5238))
    .select("city", "opened_date", "sqft")
)
