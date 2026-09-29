from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("page") == "/product") & (F.col("event_type") == "click"))
)

session_id_summary = (
    filtered_web_events
    .groupBy("session_id", "page")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    session_id_summary
    .filter(F.col("stddev_duration_sec") < 49)
    .select("session_id", "page")
    .orderBy("page", F.col("session_id").desc())
)
