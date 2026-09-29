from pyspark.sql import functions as F

min_sqft = df_stores.agg(F.min("sqft")).first()[0]

result = (
    df_stores
    .filter(F.col("sqft") <= min_sqft)
    .select("id", "sqft")
)
