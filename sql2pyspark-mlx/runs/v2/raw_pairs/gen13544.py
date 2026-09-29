from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2025-06-01")
    .select("id", "sqft")
    .orderBy(F.col("sqft").desc())
)
