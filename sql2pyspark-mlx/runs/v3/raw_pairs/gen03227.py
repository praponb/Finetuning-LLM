from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("duration_sec").between(162, 470))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .select(
        F.col("s.started_at"),
        F.col("webs.cart_duration_sec"),
        F.col("webs.avg_duration_sec"),
    )
    .orderBy(F.col("cart_duration_sec").desc(), F.col("avg_duration_sec").desc())
)
