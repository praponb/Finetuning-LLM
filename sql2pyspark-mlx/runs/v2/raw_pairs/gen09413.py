from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") > 224)
    .select("id", "session_id")
)
