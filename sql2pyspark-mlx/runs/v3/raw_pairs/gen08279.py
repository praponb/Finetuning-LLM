from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("duration_sec").between(389, 392) & F.col("session_id").isin([2, 13, 29, 30]) & F.col("duration_sec").isNotNull())
)

agg = (
    base
    .groupBy(
        F.when(F.col("duration_sec") >= 429, "high").when(F.col("duration_sec") >= 174, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    agg
    .filter(F.col("avg_duration_sec") < 420)
    .select("duration_sec_bucket", "unique_event_types")
    .orderBy(F.col("unique_event_types").desc())
)
