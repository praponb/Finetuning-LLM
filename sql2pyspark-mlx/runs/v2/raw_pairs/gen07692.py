from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter((F.col("s.device") != "desktop") | (F.col("we.event_time") < "2023-06-01"))
    .select(
        F.col("we.event_time"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.when(F.col("s.browser") == "Edge", 1).otherwise(0).alias("is_edge"),
    )
)
