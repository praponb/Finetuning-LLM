from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select("id", "browser", F.col("started_at").cast("date").alias("started_at_day"))
)
