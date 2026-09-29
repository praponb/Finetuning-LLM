from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull() | (F.col("event_type") == "purchase"))
    .withColumn("duration_sec_adjusted", F.round((F.col("duration_sec") * 100), 2))
)

summary = (
    base
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    summary
    .filter(F.col("pct_home") > 58)
    .orderBy(F.col("session_id").desc())
    .limit(3)
)
