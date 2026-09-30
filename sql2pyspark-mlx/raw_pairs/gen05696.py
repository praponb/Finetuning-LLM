from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Riverside%"))
    .orderBy(F.col("store_name").desc(), "id")
    .limit(10)
)
