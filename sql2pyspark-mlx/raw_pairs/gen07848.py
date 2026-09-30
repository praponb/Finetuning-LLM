from pyspark.sql import functions as F

max_sqft = df_stores.agg(F.max("sqft")).first()[0]

result = (
    df_stores
    .filter(F.col("sqft") >= max_sqft)
    .select("id", "sqft", "opened_date")
)
