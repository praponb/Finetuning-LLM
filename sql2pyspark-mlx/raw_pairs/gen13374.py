from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.page").isin(["/product", "/search", "/cart"]))
    .select(
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("we.event_time"),
        F.col("we.session_id"),
    )
)
