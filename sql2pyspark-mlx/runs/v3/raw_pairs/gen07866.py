from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.upper(F.trim("store_name")) == "AIRPORT KIOSK") | F.col("store_name").like("M%"))
    .select("id", "region", "sqft", F.trim("city").alias("clean_city"))
    .orderBy("id", "sqft")
)
