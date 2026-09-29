from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(9265, 12230))
    .select("opened_date")
    .unionByName(df_stores.filter(F.col("sqft") != 7426).select("opened_date"))
)
