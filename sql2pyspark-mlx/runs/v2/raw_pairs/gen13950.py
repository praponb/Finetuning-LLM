from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.session_id").isin([7, 28, 38, 40]))
    .select(
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.to_date(F.col("s.started_at")).alias("started_at_date"),
    )
    .orderBy(F.col("session_id").desc())
)
