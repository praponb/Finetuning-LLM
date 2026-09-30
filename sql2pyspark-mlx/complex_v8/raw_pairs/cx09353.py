from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_id_by_event_at = Window.partitionBy("channel", "agent_id").orderBy("event_at", "id", "event_id").rowsBetween(-2, Window.currentRow)
w_channel_agent_id_by_event_at_2 = Window.partitionBy("channel", "agent_id").orderBy("event_at", "id", "event_id")
w_event_at_dense_rank_by_csat = Window.partitionBy("event_at_dense_rank").orderBy(F.col("csat").asc_nulls_last(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_dense_rank_by_csat_2 = Window.partitionBy("event_at_dense_rank").orderBy(F.col("csat").asc_nulls_last(), "id", "event_id").rowsBetween(-6, Window.currentRow)
w_event_actor_event_id = Window.partitionBy("event_actor", "event_id")

scoped_tickets = (
    df_tickets
    .filter((F.col("priority") == "P4") & (F.col("agent_id") >= 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

line_level = (
    scoped_tickets.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
    )
)

sequenced = (
    line_level
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "event_id",
        "event_at",
        "event_type",
        F.max("requester_id").over(w_channel_agent_id_by_event_at).alias("requester_id_max_3"),
        F.dense_rank().over(w_channel_agent_id_by_event_at_2).alias("event_at_dense_rank"),
    )
)

priority_stats = (
    sequenced
    .groupBy("priority")
    .agg(F.percentile_approx("csat", 0.5).alias("median_csat"))
)

vs_group = (
    sequenced.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.csat") >= F.col("g.median_csat"))
    .select(
        "s.*",
        "g.median_csat",
        F.round(F.col("s.csat") - F.col("g.median_csat"), 2).alias("csat_minus_median"),
    )
)

with_parent = (
    vs_group.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .filter(F.col("su.tier").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.event_id",
        "s.event_at",
        "s.event_type",
        "s.event_at_dense_rank",
        "s.median_csat",
        "s.csat_minus_median",
        F.col("su.tier").alias("agent_tier"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

sequenced_2 = (
    with_parent
    .select(
        "*",
        F.nth_value("opened_at", 2).over(w_event_at_dense_rank_by_csat).alias("second_opened_at"),
        F.sum("csat").over(w_event_at_dense_rank_by_csat_2).alias("csat_sum_7"),
        F.round(F.avg("csat").over(w_event_at_dense_rank_by_csat_2), 2).alias("csat_avg_7"),
        F.size(F.collect_set("event_type").over(w_event_actor_event_id)).alias("distinct_event_type_in_event_actor_event_id"),
    )
)

result = (
    sequenced_2
    .filter(F.col("event_event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "event_id",
        "event_at_dense_rank",
        "median_csat",
        "csat_minus_median",
        "agent_tier",
        "event_actor",
        "distinct_event_type_in_event_actor_event_id",
        (F.col("opened_at") + F.expr("INTERVAL 30 MINUTES")).alias("opened_at_plus_30m"),
    )
    .orderBy(F.col("priority").desc())
)
