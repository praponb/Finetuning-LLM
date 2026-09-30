from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([21, 33, 35, 40]) & F.col("duration_sec").between(387, 479))
    .groupBy("event_type", F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
    .filter(F.col("unique_sessions") > 4)
    .orderBy(F.col("pct_search").desc())
)
