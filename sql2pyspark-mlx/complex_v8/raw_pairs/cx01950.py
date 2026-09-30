from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_events = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("|", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_prev = valid_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_actor_running))

session_summary = (
    sessionized
    .groupBy("actor", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    session_summary
    .select(
        "*",
        (F.col("duration_min") + F.col("session_id")).alias("duration_min_plus_session_id"),
        F.when(F.col("duration_min") >= 192.0, "large").when(F.col("duration_min") >= 108.0, "medium").otherwise("small").alias("duration_min_tier"),
    )
)

enriched = (
    labeled
    .filter(F.col("session_start") >= F.lit("2025-01-09 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("duration_min_tier").isin("medium", "small"), "group_a").when(F.col("duration_min_tier") == "large", "group_b").otherwise("other").alias("duration_min_tier_group"),
        (F.col("session_start") + F.expr("INTERVAL 1 DAYS")).alias("session_start_plus_1d"),
    )
)

result = (
    enriched
    .select(
        "actor",
        "session_start",
        "session_end",
        "duration_min",
        "duration_min_plus_session_id",
        "duration_min_tier",
        "duration_min_tier_group",
        "session_start_plus_1d",
        F.abs(F.col("duration_min") - 144.0).alias("duration_min_distance"),
    )
    .orderBy(F.col("actor").desc())
)
