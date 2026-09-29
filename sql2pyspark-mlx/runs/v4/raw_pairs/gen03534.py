from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "sqft")
    .orderBy(F.col("id").desc())
)
