from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_agent_team_by_agent = Window.partitionBy("agent_id", "agent_team").orderBy("agent_hired_on", "id")
w_agent_id_agent_team_by_agent_2 = Window.partitionBy("agent_id", "agent_team").orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_hired_on_month_agent_t = Window.partitionBy(F.date_trunc("month", "agent_hired_on"), "agent_team").orderBy("agent_hired_on")
w_agent_id_agent_team_by_agent_3 = Window.partitionBy("agent_id", "agent_team").orderBy("agent_hired_on")

valid_tickets = df_tickets.filter(F.col("priority") == "P2")

scored = (
    valid_tickets
    .filter(F.col("csat").between(2, 4))
    .select(
        "*",
        (F.col("opened_at") + F.expr("INTERVAL 30 MINUTES")).alias("opened_at_plus_30m"),
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
    )
)

with_parent_agent = (
    scored.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        "s.opened_at_plus_30m",
        "s.opened_at_date",
        "s.channel_prefix",
        F.col("su.tier").alias("agent_tier"),
    )
)

with_parent = (
    with_parent_agent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (~F.col("su.tier").isin("T1", "T3")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.tags",
        "s.agent_tier",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "agent_id",
        "tags",
        "agent_tier",
        "agent_hired_on",
        F.rank().over(w_agent_id_agent_team_by_agent).alias("agent_hired_on_rank"),
        F.last("agent_hired_on", ignorenulls=True).over(w_agent_id_agent_team_by_agent_2).alias("last_known_agent_hired_on"),
        F.round(F.percent_rank().over(w_agent_hired_on_month_agent_t), 3).alias("agent_hired_on_percent_rank"),
        F.rank().over(w_agent_id_agent_team_by_agent_3).alias("agent_hired_on_rank_2"),
        F.rank().over(w_agent_hired_on_month_agent_t).alias("agent_hired_on_rank_3"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.array_contains(F.col("t.tags"), "bug")) & (F.col("t.csat") >= 1), "left_anti")
)

joined_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.tags",
        "s.agent_hired_on",
        "s.agent_hired_on_percent_rank",
        "s.agent_hired_on_rank_2",
        "s.agent_hired_on_rank_3",
        "su.agent_name",
    )
)

result = (
    joined_agent
    .filter(F.lower(F.col("agent_name")).like("%ir%"))
    .select(
        "id",
        "agent_id",
        "agent_hired_on_rank_2",
        "agent_hired_on_rank_3",
        "agent_name",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("agent_hired_on")).alias("days_since_agent_hired_on"),
        F.date_format(F.col("agent_hired_on"), "yyyy-MM-dd").alias("agent_hired_on_label"),
    )
    .orderBy(F.col("agent_name").desc_nulls_first())
)
