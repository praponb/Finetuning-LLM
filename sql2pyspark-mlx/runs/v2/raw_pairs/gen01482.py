from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper(F.trim("store_name")) == "AIRPORT KIOSK")
    .select("id", "city", "region", "sqft", F.floor("sqft").alias("sqft_whole"))
    .orderBy(F.col("sqft_whole").desc(), "city")
)
