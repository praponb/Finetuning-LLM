from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id = Window.partitionBy("event_type", "ticket_id")
w_all_by_days_since_event_at_r = Window.orderBy(F.col("days_since_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

windowed = (
    recent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_slug",
        "actor_ref",
        F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_event_type_ticket_id), 2).alias("days_since_event_at_vs_event_type_ticket_id_avg"),
        F.last("days_since_event_at", ignorenulls=True).over(w_all_by_days_since_event_at_r).alias("last_known_days_since_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

summarized_rows = (
    windowed.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

combined_details_ticket = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_slug",
        "s.days_since_event_at_vs_event_type_ticket_id_avg",
        "s.last_known_days_since_event_at",
        "s.last_event_at",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

result = (
    combined_details_ticket
    .filter(F.col("event_type_slug").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_type_slug",
        "last_known_days_since_event_at",
        "last_event_at",
        "ticket_channel",
        "ticket_agent_id",
        "ticket_opened_at",
    )
    .orderBy(
        F.col("ticket_channel").asc(),
        F.col("last_known_days_since_event_at").desc(),
        "id",
    )
    .limit(10)
)
