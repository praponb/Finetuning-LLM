from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(~F.col("we.event_type").isin(["add_to_cart", "purchase"]))
    .select(
        F.col("s.device"),
        F.col("we.event_time"),
        F.col("we.duration_sec"),
        F.col("we.event_type"),
        F.when(F.col("we.event_type") == "click", 1).otherwise(0).alias("is_click"),
    )
    .orderBy(F.col("duration_sec").desc(), "device")
)
