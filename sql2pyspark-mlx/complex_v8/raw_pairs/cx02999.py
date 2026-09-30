from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_tier").orderBy(F.col("priority_len").desc(), F.col("id").desc())

scoped = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        F.year(F.col("closed_at")).alias("closed_at_year"),
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

joined = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.closed_at_year",
        "s.priority_prefix",
        "s.priority_len",
        F.col("su.tier").alias("agent_tier"),
    )
)

deduped = (
    joined
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

with_counts = (
    deduped.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.last_event_at",
        "t.first_event_at_2",
    )
)

with_children = (
    with_counts.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.priority_prefix",
        "s.priority_len",
        "s.agent_tier",
        "s.first_event_at_2",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    with_children
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "priority_prefix",
        "priority_len",
        "event_id",
        "event_at",
        "event_type",
        "event_actor",
        F.concat_ws(": ", F.col("priority_prefix"), F.col("event_actor")).alias("priority_prefix_event_actor_label"),
        (F.col("opened_at") + F.expr("INTERVAL 15 MINUTES")).alias("opened_at_plus_15m"),
    )
    .orderBy(F.col("priority_len").desc_nulls_first())
)
