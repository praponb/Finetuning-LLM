from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "SG")
    .select("id", "country", "device", F.trim("country").alias("clean_country"))
)
