from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2025-05-01")
    .select("id", "browser", "started_at", "device")
)
