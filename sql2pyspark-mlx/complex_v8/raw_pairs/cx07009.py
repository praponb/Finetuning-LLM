from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_channel_by_event_at = Window.partitionBy("ticket_channel").orderBy(F.col("event_at").desc(), "id")

scoped = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

combined_details_ticket = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_label",
        "s.actor_clean",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

with_windows = (
    combined_details_ticket
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_event_at), 3).alias("event_at_percent_rank"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.lead("event_at").over(w_ticket_channel_by_event_at).alias("next_event_at"),
    )
)

line_level = (
    with_windows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.ticket_channel",
        "s.ticket_tags",
        "s.event_at_percent_rank",
        "s.first_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    line_level
    .cube("event_actor", "ticket_channel")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("event_id").alias("distinct_event_ids"),
    )
    .select(
        F.coalesce(F.col("event_actor"), F.lit("ALL")).alias("event_actor"),
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "cnt",
        "distinct_event_ids",
    )
)
