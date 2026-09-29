from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("s.device") == "mobile")
    .groupBy(F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.when(F.col("we.event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
    )
    .filter(F.col("purchase_count") >= 70)
    .orderBy(F.col("purchase_count").desc(), "browser")
    .limit(5)
)
