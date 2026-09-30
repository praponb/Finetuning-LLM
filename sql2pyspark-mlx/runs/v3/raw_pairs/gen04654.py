from pyspark.sql import functions as F

avg_sqft = df_stores.agg(F.avg("sqft")).first()[0]

result = (
    df_stores
    .filter(F.col("sqft") > avg_sqft)
    .select("id", "sqft", "city", "store_name")
)
