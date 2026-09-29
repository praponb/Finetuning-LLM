from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .intersect(df_stores.filter(F.col("sqft").between(5482, 11191)).select("city"))
    .orderBy(F.col("city").desc())
)
