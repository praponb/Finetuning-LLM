from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Chiang Mai", "Singapore"]) & F.col("store_name").isNotNull())
    .select("id", "sqft", "region")
)
