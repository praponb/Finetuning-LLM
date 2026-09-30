from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "Chiang Mai")
    .select("id", "sqft", "city")
    .unionByName(df_stores.filter(F.col("sqft") > 15107).select("id", "sqft", "city"))
    .orderBy("sqft")
)
