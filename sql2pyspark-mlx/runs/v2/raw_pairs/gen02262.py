from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "US")
    .select("started_at", F.lower("browser").alias("browser_lower"))
)
