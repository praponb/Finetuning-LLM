from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("channel").orderBy(F.col("requester_id").desc(), F.col("id").desc())
w_channel_by_closed_at_prior_2 = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(-2, -1)
w_channel_by_closed_at = Window.partitionBy("channel").orderBy("closed_at", "id")

base = (
    df_tickets
    .filter((F.col("priority").isin("P1", "P2", "P3")) & (F.col("closed_at") >= F.lit("2025-02-02 05:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

without_matches = (
    base.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isNotNull()) & (F.col("su.hired_on").isNotNull()), "left_semi")
)

deduped = (
    without_matches
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

summarized_rows = (
    deduped.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
    )
)

combined_details_agent = (
    summarized_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.event_count",
        "s.last_event_at",
        "su.agent_name",
    )
)

with_parent = (
    combined_details_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") == "Billing")
    .select("s.*", F.col("su.team").alias("agent_team"))
)

metrics = (
    with_parent
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "last_event_at",
        "agent_name",
        "agent_team",
        F.round(F.avg("event_count").over(w_channel_by_closed_at_prior_2), 2).alias("event_count_avg_2_prior"),
        F.lead("event_count").over(w_channel_by_closed_at).alias("next_event_count"),
    )
)

result = (
    metrics
    .select("id", "csat", "next_event_count")
    .unpivot(
        ids=["id"],
        values=["csat", "next_event_count"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
