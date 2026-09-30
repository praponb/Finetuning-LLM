from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") > 254) & (F.col("page") == "/search") & (F.col("session_id") == 14))
    .select("id", "page", "duration_sec")
)
