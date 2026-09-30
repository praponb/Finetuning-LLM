from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 28)
    .select("page", "duration_sec")
    .orderBy(F.col("duration_sec").desc())
)
