from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser").isin(["Firefox", "Chrome", "Edge"]) | F.col("s.browser").isin(["Chrome", "Edge", "Safari"]))
    .select(
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("we.event_type"),
    )
)
