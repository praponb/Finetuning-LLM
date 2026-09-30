from pyspark.sql import functions as F

result = (
    df_stores
    .select("city")
    .distinct()
    .orderBy(F.col("city").desc())
    .limit(20)
)
