from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("channel").orderBy("requester_id", "id")
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc())
w_opened_at_to_date_priority = Window.partitionBy(F.to_date("opened_at"), "priority")

prepared = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 1) & (F.col("channel").isNotNull()))
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
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
    )
)

deduped = (
    prepared
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    deduped
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.rank().over(w_all_by_opened_at_ties).alias("opened_at_rank"),
        F.round(F.col("csat") - F.avg("csat").over(w_opened_at_to_date_priority), 2).alias("csat_vs_opened_at_to_date_priority_avg"),
    )
)

enriched_agent = (
    windowed.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (~F.col("su.tier").isin("T3", "T2")), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.tags",
        "s.opened_at_rank",
        "s.csat_vs_opened_at_to_date_priority_avg",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_counts = (
    enriched_agent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

agent_id_stats = (
    with_counts
    .groupBy("agent_id")
    .agg(F.percentile_approx("csat_vs_opened_at_to_date_priority_avg", 0.5).alias("median_csat_vs_opened_at_to_date_priority_avg"))
)

compared = (
    with_counts.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat_vs_opened_at_to_date_priority_avg") >= F.col("g.median_csat_vs_opened_at_to_date_priority_avg"))
    .select(
        "s.*",
        "g.median_csat_vs_opened_at_to_date_priority_avg",
        F.round(F.col("s.csat_vs_opened_at_to_date_priority_avg") - F.col("g.median_csat_vs_opened_at_to_date_priority_avg"), 2).alias("csat_vs_opened_at_to_date_priority_avg_minus_med"),
    )
)

with_details_agent = (
    compared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.tags",
        "s.opened_at_rank",
        "s.agent_hired_on",
        "s.ticket_count",
        "s.median_csat_vs_opened_at_to_date_priority_avg",
        F.col("su.hired_on").alias("agent_hired_on_2"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_details_agent
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at_rank",
        "agent_hired_on",
        "ticket_count",
        "agent_hired_on_2",
        "agent_tier",
    )
    .orderBy(F.col("agent_tier").desc(), "ticket_count")
)
