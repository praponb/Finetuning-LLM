from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.device").isin(["tablet", "mobile", "desktop"]) & F.col("s.customer_id").isin([7, 27, 35]))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.event_type"),
        F.col("s.browser"),
    )
    .orderBy("event_type", F.col("id").desc())
)
