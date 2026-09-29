from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

with_prev = recent_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_ticket_id_running))

sessions = (
    sessionized
    .groupBy("ticket_id", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    sessions
    .select(
        "ticket_id",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        F.abs(F.col("duration_min") - 72.0).alias("duration_min_distance"),
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count_2"),
        F.count("*").alias("event_count_3"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    labeled.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
        F.coalesce(F.col("t.event_count_3"), F.lit(0)).alias("event_count_3"),
        "t.last_event_at",
    )
)

result = (
    with_stats
    .rollup("event_count_2", "ticket_id", "event_count_3")
    .agg(
        F.grouping("event_count_2").alias("is_total_event_count_2"),
        F.min("session_start").alias("first_session_start"),
    )
)
