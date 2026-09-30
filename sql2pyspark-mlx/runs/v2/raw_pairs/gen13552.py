from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 15439)
    .select("id", "region")
    .unionByName(df_stores.filter(F.col("sqft") < 7764).select("id", "region"))
)
