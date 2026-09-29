from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(10111, 14471))
    .select("id", "region", "sqft")
    .orderBy(F.col("region").desc())
)
