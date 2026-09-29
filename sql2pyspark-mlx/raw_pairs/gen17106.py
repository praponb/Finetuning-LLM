from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower("store_name").like("%all%"))
    .select("id", "region", "sqft", "store_name", F.floor("sqft").alias("sqft_whole"))
)
