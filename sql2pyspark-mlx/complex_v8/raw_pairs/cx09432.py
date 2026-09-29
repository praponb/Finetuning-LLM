from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter(F.col("actor").isin("customer", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_actor_running))

line_level_events = (
    sessionized.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("escalated", "resolved", "replied")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.prev_event_at",
        "s.starts_session",
        "s.burst_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

enriched = (
    line_level_events
    .filter(F.col("starts_session") >= 0)
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "starts_session",
        "burst_id",
        "event_id",
        "event_event_at",
        F.concat_ws(": ", F.col("event_type"), F.col("event_event_type")).alias("event_type_event_event_type_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("prev_event_at")).alias("days_since_prev_event_at"),
        F.concat_ws("-", F.col("event_type"), F.col("event_event_type")).alias("event_type_event_event_type_label_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "starts_session",
        "burst_id",
        "event_event_at",
        "event_type_event_event_type_label",
        "days_since_prev_event_at",
        "event_type_event_event_type_label_2",
        F.when(F.col("event_type").isin("replied", "created"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
    )
    .orderBy(F.col("ticket_id").desc())
)
