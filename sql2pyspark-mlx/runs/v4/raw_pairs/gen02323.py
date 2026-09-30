from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Berlin", "Singapore"]))
    .select("id", "region", "store_name", "sqft")
    .orderBy(F.col("sqft").desc(), "store_name")
)
