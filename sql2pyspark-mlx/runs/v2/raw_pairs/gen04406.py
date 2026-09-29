from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") < 12598)
    .select("sqft")
    .orderBy(F.col("sqft").desc())
)
