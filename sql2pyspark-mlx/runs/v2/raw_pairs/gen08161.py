from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(8359, 11602))
    .select("id", "city", "sqft")
    .orderBy(F.col("sqft").desc(), "id")
    .limit(5)
)
