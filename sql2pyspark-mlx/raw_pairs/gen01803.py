from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("s.country").isin(["JP", "TH"]))
    .groupBy(F.to_date(F.col("s.started_at")).alias("started_day"))
    .agg(
        F.count(F.when(F.col("we.page") == "/checkout", 1)).alias("checkout_count"),
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
    )
    .orderBy(F.col("started_day").desc())
)
