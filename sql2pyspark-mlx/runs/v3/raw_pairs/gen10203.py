from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper(F.trim("store_name")) == "OLD TOWN")
    .select("sqft", "region")
    .unionByName(df_stores.filter(F.col("sqft") > 5552).select("sqft", "region"))
    .orderBy("region")
)
