from pyspark.sql import functions as F

min_sqft = df_stores.filter(F.col("sqft").between(4790, 8439)).agg(F.min("sqft")).first()[0]

result = (
    df_stores
    .filter(F.col("sqft") < min_sqft)
    .select("id", "sqft")
)
