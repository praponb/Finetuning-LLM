from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_closed_at_year_agent_id_by_o = Window.partitionBy(F.year("closed_at"), "agent_id").orderBy(F.col("opened_at").desc())
w_closed_at_year_agent_id_by_o_2 = Window.partitionBy(F.year("closed_at"), "agent_id").orderBy(F.col("opened_at").desc(), "id", "tag")

scoped = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "outage"))
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
    )
)

channel_stats = (
    scoped
    .groupBy("channel")
    .agg(F.avg("csat").alias("avg_csat"))
)

vs_group = (
    scoped.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

per_label = (
    vs_group
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "avg_csat",
        "csat_minus_avg",
        F.explode("tags").alias("tag"),
    )
)

windowed = (
    per_label
    .select(
        "*",
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.round(F.cume_dist().over(w_closed_at_year_agent_id_by_o), 3).alias("opened_at_cume_dist"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat), F.lit(0)), 4).alias("csat_share_of_csat"),
        F.round(F.percent_rank().over(w_closed_at_year_agent_id_by_o_2), 3).alias("opened_at_percent_rank"),
        F.min("avg_csat").over(w_csat).alias("csat_min_avg_csat"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.max("opened_at").alias("last_ticket_opened_at_2"),
    )
)

summarized_rows = (
    windowed.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.last_ticket_opened_at",
        "t.last_ticket_closed_at",
        "t.last_ticket_opened_at_2",
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ticket_stats_2 = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.round(F.avg("csat"), 2).alias("avg_ticket_csat"),
        F.count(F.when(F.col("channel").isNotNull(), 1)).alias("ticket_channel_count"),
    )
)

with_stats = (
    combined_details.alias("s")
    .join(ticket_stats_2.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.first_ticket_closed_at",
        "t.avg_ticket_csat",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
    )
)

result = (
    with_stats
    .filter(F.col("opened_at_percent_rank") >= 0.2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "avg_csat",
        "csat_minus_avg",
        "tag",
        "rows_in_csat",
        "opened_at_cume_dist",
        "csat_share_of_csat",
        "last_ticket_closed_at",
        "last_ticket_opened_at_2",
        "agent_name",
        "agent_tier",
        "first_ticket_closed_at",
        "avg_ticket_csat",
        "ticket_channel_count",
        F.substring(F.trim(F.col("agent_tier")), 1, 4).alias("agent_tier_prefix"),
    )
)
