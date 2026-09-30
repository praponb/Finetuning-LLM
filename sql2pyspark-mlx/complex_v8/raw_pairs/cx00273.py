from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 20)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = valid.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_event_type_running))

line_level_events = (
    sessionized.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.prev_event_at",
        "s.starts_session",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

event_actor_stats = (
    line_level_events
    .groupBy("event_actor")
    .agg(F.percentile_approx("starts_session", 0.5).alias("median_starts_session"))
)

vs_group = (
    line_level_events.alias("s")
    .join(event_actor_stats.alias("g"), F.col("g.event_actor") == F.col("s.event_actor"), "inner")
    .filter(F.col("s.starts_session") >= F.col("g.median_starts_session"))
    .select(
        "s.*",
        "g.median_starts_session",
        F.round(F.col("s.starts_session") - F.col("g.median_starts_session"), 2).alias("starts_session_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "ticket_id",
        "event_at",
        "prev_event_at",
        "starts_session",
        "event_id",
        "event_actor",
        "event_event_at",
        "starts_session_minus_median",
    )
)
