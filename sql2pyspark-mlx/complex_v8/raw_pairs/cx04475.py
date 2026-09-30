from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier_group_first_close = Window.partitionBy("agent_tier_group", F.date_trunc("month", "first_closed_at")).orderBy(F.col("first_closed_at").asc_nulls_last())
w_agent_hired_on_month_ticket = Window.partitionBy(F.month("agent_hired_on"), F.date_trunc("month", "ticket_opened_at"))
w_agent_tier_group_first_close_2 = Window.partitionBy("agent_tier_group", F.date_trunc("month", "first_closed_at")).orderBy(F.col("first_closed_at").asc_nulls_last(), "agent_id", "opened_at_month", "csat", "ticket_id").rowsBetween(-6, -1)

recent_tickets = (
    df_tickets
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

rollup_base = (
    recent_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-12 10:00:00").cast("timestamp"))
    .groupBy(
        "agent_id",
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
        "csat",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("csat").alias("max_csat"),
        F.min("closed_at").alias("first_closed_at"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

with_parent = (
    rollup_base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier") != "T1")
    .select("s.*", "su.agent_name", F.col("su.tier").alias("agent_tier"))
)

tagged = (
    with_parent
    .select(
        "agent_id",
        "opened_at_month",
        "csat",
        "max_csat",
        "first_closed_at",
        "distinct_channels",
        "distinct_ids",
        "agent_name",
        "agent_tier",
        F.initcap(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
        F.when(F.col("agent_tier").isin("T2", "T1"), "group_a").when(F.col("agent_tier") == "T3", "group_b").otherwise("other").alias("agent_tier_group"),
    )
)

enriched_agent = (
    tagged.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.agent_id",
        "s.opened_at_month",
        "s.csat",
        "s.first_closed_at",
        "s.distinct_channels",
        "s.agent_name",
        "s.agent_tier",
        "s.agent_name_clean",
        "s.agent_tier_group",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier_2"),
        F.col("su.team").alias("agent_team"),
    )
)

line_level = (
    enriched_agent.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.to_date(F.col("t.closed_at")) < F.lit("2025-03-08").cast("date")), "left")
    .select(
        "s.agent_id",
        "s.opened_at_month",
        "s.csat",
        "s.first_closed_at",
        "s.agent_name",
        "s.agent_tier_group",
        "s.agent_hired_on",
        F.col("t.id").alias("ticket_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

sequenced = (
    line_level
    .select(
        "*",
        F.round(F.percent_rank().over(w_agent_tier_group_first_close), 3).alias("first_closed_at_percent_rank"),
        F.sum("csat").over(w_agent_hired_on_month_ticket).alias("agent_hired_on_month_ticket_opened_at_month_sum_"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_hired_on_month_ticket), F.lit(0)), 4).alias("csat_share_of_agent_hired_on_month_ticket_opened"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_hired_on_month_ticket), 2).alias("csat_vs_agent_hired_on_month_ticket_opened_at_mo"),
        F.max("csat").over(w_agent_tier_group_first_close_2).alias("csat_max_6_prior"),
    )
)

result = (
    sequenced
    .filter(F.col("opened_at_month") >= F.lit("2025-01-09 09:00:00").cast("timestamp"))
    .select(
        "agent_id",
        "opened_at_month",
        "csat",
        "first_closed_at",
        "agent_name",
        "ticket_id",
        "ticket_closed_at",
        "ticket_opened_at",
        "first_closed_at_percent_rank",
        "csat_share_of_agent_hired_on_month_ticket_opened",
        "csat_vs_agent_hired_on_month_ticket_opened_at_mo",
        "csat_max_6_prior",
        F.lower(F.trim(F.col("agent_tier_group"))).alias("agent_tier_group_clean"),
    )
    .orderBy(F.col("ticket_opened_at").desc())
)
