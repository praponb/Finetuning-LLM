from pyspark.sql import functions as F

matched_keys = (
    df_web_events
    .select("event_type", "id")
    .intersect(
        df_web_events
        .filter(F.col("event_time") >= "2023-04-03")
        .select("event_type", "id")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("duration_sec") >= 526, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.min("event_time").alias("started_at"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
