from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 15865)
    .select("id", "sqft", "opened_date", "city")
    .orderBy(F.col("sqft").desc())
)
