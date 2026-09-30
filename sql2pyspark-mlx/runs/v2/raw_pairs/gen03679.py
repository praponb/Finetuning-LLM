from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(5602, 10237) & F.col("region").isNotNull())
    .select("city")
    .orderBy(F.col("city").desc())
)
