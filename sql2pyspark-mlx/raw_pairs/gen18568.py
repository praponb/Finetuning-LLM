from pyspark.sql import functions as F

base = df_web_events.filter(F.col("event_time") >= "2023-12-04")

session_id_summary = (
    base
    .groupBy("session_id", "event_type")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cart"),
    )
)

result = session_id_summary.select("session_id", "event_type", "duration_sec_count")
