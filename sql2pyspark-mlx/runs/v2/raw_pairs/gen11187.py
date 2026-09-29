from pyspark.sql import functions as F

min_sqft = df_stores.filter(F.month("opened_date").isin([5, 7, 11])).agg(F.min("sqft")).first()[0]

result = (
    df_stores
    .filter(F.col("sqft") == min_sqft)
    .select("id", "sqft", "store_name")
    .orderBy(F.col("sqft").desc())
)
