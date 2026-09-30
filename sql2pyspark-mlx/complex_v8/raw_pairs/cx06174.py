from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_requester_id_full = Window.partitionBy("csat").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_event_event_type_co = Window.partitionBy("agent_id", "event_event_type_count")

base = (
    df_tickets
    .filter((~F.array_contains(F.col("tags"), "bug")) & (F.col("agent_id") <= 8))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("event_event_type_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    base.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.last_event_at",
    )
)

sequenced = (
    with_stats
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "hours_opened_at_to_closed_at",
        "priority_clean",
        "event_event_type_count",
        F.last("closed_at").over(w_csat_by_requester_id_full).alias("last_closed_at"),
        F.size(F.collect_set("priority").over(w_agent_id_event_event_type_co)).alias("distinct_priority_in_agent_id_event_event_type_c"),
    )
)

with_children_events = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (~F.col("t.event_type").isin("replied", "created")), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.priority",
        "s.csat",
        "s.hours_opened_at_to_closed_at",
        "s.last_closed_at",
        "s.distinct_priority_in_agent_id_event_event_type_c",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
    )
)

detail_rows_events = (
    with_children_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.priority",
        "s.csat",
        "s.hours_opened_at_to_closed_at",
        "s.distinct_priority_in_agent_id_event_event_type_c",
        "s.event_id",
        "s.event_type",
        "s.event_at",
        F.col("t.id").alias("event_id_2"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    detail_rows_events
    .filter(F.col("priority") == "P4")
    .select(
        "id",
        "requester_id",
        "priority",
        "csat",
        "hours_opened_at_to_closed_at",
        "event_type",
        "event_id_2",
        "event_event_type",
    )
    .orderBy(
        F.col("hours_opened_at_to_closed_at").desc(),
        F.col("event_type").desc_nulls_first(),
    )
)
