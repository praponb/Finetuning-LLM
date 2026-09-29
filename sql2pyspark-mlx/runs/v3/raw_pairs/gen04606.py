from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("session_id").isin([25, 27]) & (F.col("page") == "/cart")) | F.col("duration_sec").between(335, 419))
)

event_type_summary = (
    recent_web_events
    .groupBy("event_type")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
    )
)

result = event_type_summary
