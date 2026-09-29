from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cart"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.duration_sec_range") > 67)
    .select(F.col("s.country"), F.col("t.duration_sec_range"), F.col("t.pct_cart"))
    .orderBy("pct_cart")
)
