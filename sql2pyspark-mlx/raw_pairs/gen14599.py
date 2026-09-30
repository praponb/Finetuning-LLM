from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Berlin", "Bangkok", "New York"]))
    .select("region")
    .unionByName(df_stores.filter(F.col("sqft").between(6088, 12362)).select("region"))
    .orderBy(F.col("region").desc())
)
