from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser").isin(["Edge", "Safari"]))
    .groupBy(F.col("s.device"))
    .agg(
        F.count(F.when(F.col("we.page") == "/cart", 1)).alias("cart_count"),
        F.sum(F.col("we.duration_sec")).alias("total_duration_sec"),
    )
    .orderBy("total_duration_sec")
)
