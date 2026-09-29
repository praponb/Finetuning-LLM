from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "city")
    .orderBy(F.col("id").desc())
    .limit(1)
)
