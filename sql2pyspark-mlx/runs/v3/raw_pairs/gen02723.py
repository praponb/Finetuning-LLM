from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Ai%") & (F.col("region") != "North"))
    .select("id", "store_name", F.trim("city").alias("clean_city"))
)
