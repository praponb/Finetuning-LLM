from pyspark.sql import functions as F

avg_sqft = df_stores.filter(F.col("opened_date") >= "2024-01-01").agg(F.avg("sqft")).first()[0]

result = (
    df_stores
    .filter(F.col("sqft") >= avg_sqft)
    .select("id", "sqft", "city", "region")
    .orderBy(F.col("id").desc())
)
