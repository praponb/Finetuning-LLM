from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "purchase") & (F.col("duration_sec") < 304))
    .select("id", "session_id", F.trim("page").alias("clean_page"))
    .orderBy(F.col("id").desc())
)
