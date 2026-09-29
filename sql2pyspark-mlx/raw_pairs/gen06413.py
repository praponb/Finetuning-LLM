from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.duration_sec") < 443)
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.date_trunc("MONTH", F.col("s.started_at")).alias("started_at_period"),
    )
)
