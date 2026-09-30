from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2024-01-01")
    .select("id", "country", "browser", F.upper("browser").alias("browser_upper"))
)
