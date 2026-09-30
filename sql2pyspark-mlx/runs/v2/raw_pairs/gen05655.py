from pyspark.sql import functions as F

result = (
    df_stores
    .select("region", "city")
    .distinct()
    .orderBy(F.col("region").desc(), "city")
    .limit(50)
)
