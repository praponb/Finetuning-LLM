from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet", "mobile"]) | F.col("device").isin(["desktop", "tablet", "mobile"]))
    .select("id", "country", "device", "started_at")
)
