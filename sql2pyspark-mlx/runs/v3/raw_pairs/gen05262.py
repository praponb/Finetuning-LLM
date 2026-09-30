from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.page").isin(["/home", "/product", "/cart"]) | F.col("s.country").isin(["DE", "TH"]))
    .select(
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("we.event_time"),
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.date_trunc("month", F.col("we.event_time")).alias("event_time_period"),
    )
    .orderBy(F.col("customer_id").desc())
)
