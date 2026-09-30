from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 2)
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"), "session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
    .filter(F.col("pct_purchase") > 25)
    .orderBy(F.col("pct_purchase").desc())
)
