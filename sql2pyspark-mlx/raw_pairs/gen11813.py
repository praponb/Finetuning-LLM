from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "North")
    .select("id", "sqft")
    .unionByName(df_stores.filter(F.length("store_name") > 13).select("id", "sqft"))
    .distinct()
    .orderBy(F.col("sqft").desc())
)
