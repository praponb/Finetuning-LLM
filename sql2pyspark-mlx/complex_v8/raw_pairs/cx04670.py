from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_channel = Window.partitionBy("actor", "ticket_channel")
w_ticket_channel_ticket_tag_by = Window.partitionBy("ticket_channel", "ticket_tag").orderBy("ticket_csat", "id", "ticket_tag").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

combined_details = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.closed_at") >= F.lit("2025-01-14 23:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_slug",
        "s.event_at_date",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

flattened = (
    combined_details
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_slug",
        "event_at_date",
        "ticket_csat",
        "ticket_channel",
        F.explode("ticket_tags").alias("ticket_tag"),
    )
)

with_windows = (
    flattened
    .withColumn("actor_ticket_channel_sum_ticket_csat", F.sum("ticket_csat").over(w_actor_ticket_channel))
    .withColumn("first_event_at_date", F.first("event_at_date").over(w_ticket_channel_ticket_tag_by))
)

with_parent_ticket = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.col("t.priority") == "P3")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_slug",
        "s.event_at_date",
        "s.ticket_csat",
        "s.ticket_channel",
        "s.ticket_tag",
        "s.actor_ticket_channel_sum_ticket_csat",
        "s.first_event_at_date",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
        "su.agent_name",
    )
)

result = (
    with_parent_ticket
    .groupingSets(
        [["ticket_csat", "ticket_tag"], ["ticket_csat"], ["ticket_tag"], []],
        "ticket_csat", "ticket_tag",
    )
    .agg(
        F.round(F.avg("ticket_csat"), 2).alias("avg_ticket_csat"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("ticket_csat").asc_nulls_last(),
        F.col("ticket_tag").asc_nulls_last(),
    )
)
