from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "mobile") & ((F.col("started_at") >= "2023-11-01") | (F.col("started_at") >= "2023-12-01")))
    .withColumn("browser_lower", F.lower("browser"))
)
