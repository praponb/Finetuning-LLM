from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_actor", "event_ticket_id").orderBy(F.col("requester_id").desc(), F.col("id").desc(), F.col("event_id").desc())

recent = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
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
        F.concat_ws("-", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

detail_rows = (
    recent.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

labeled = (
    detail_rows
    .filter(F.col("channel") == "phone")
    .withColumn("requester_id_filled", F.coalesce(F.col("requester_id"), F.lit(0)))
)

with_parent = (
    labeled.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("t.ticket_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.event_id",
        "s.event_type",
        "s.event_actor",
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.ticket_id").alias("event_ticket_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("ti.priority").alias("ticket_priority"),
        F.col("ti.tags").alias("ticket_tags"),
    )
)

latest_per_key = (
    with_parent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

per_group = (
    latest_per_key
    .groupBy("event_actor_2")
    .agg(
        F.array_sort(F.collect_set("event_actor_2")).alias("event_actor_2_list"),
        F.array_sort(F.collect_set("ticket_priority")).alias("ticket_priority_list"),
        F.sum(F.when(F.col("priority").isNotNull(), F.col("requester_id"))).alias("requester_id_priority_is_not_null"),
        F.max("requester_id").alias("max_requester_id"),
    )
)

result = (
    per_group
    .select(
        "event_actor_2",
        "event_actor_2_list",
        "ticket_priority_list",
        "requester_id_priority_is_not_null",
        "max_requester_id",
        F.substring(F.trim(F.col("event_actor_2")), 1, 3).alias("event_actor_2_prefix"),
        F.length(F.trim(F.col("event_actor_2"))).alias("event_actor_2_len"),
    )
    .orderBy("event_actor_2_len")
)
