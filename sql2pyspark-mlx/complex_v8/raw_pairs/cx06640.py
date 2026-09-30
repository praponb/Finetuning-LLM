from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team = Window.partitionBy("agent_team")
w_agent_team_priority_by_close = Window.partitionBy("agent_team", "priority").orderBy("closed_at", "id")
w_agent_team_priority_by_close_2 = Window.partitionBy("agent_team", "priority").orderBy("closed_at", "id").rowsBetween(-2, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 2)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat_ws("|", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

qualified = (
    scoped_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.array_contains(F.col("t.tags"), "login")) & (F.col("t.requester_id") < 19), "left_semi")
    .filter(F.col("channel").isin("web", "phone", "chat"))
)

screened = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

with_parent = (
    screened.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        F.col("su.team").alias("agent_team"),
    )
)

with_windows = (
    with_parent
    .select(
        "*",
        F.round(F.avg("csat").over(w_agent_team), 2).alias("agent_team_avg_csat"),
        F.lead("csat", 3).over(w_agent_team_priority_by_close).alias("next3_csat"),
        F.lag("csat", 2).over(w_agent_team_priority_by_close).alias("prev2_csat"),
        F.round(F.avg("csat").over(w_agent_team_priority_by_close_2), 2).alias("csat_avg_3"),
    )
)

per_group = (
    with_windows
    .groupBy("priority", "next3_csat", "prev2_csat")
    .agg(
        F.round(100 * F.count(F.when(~F.col("agent_team").isin("VIP", "Onboarding"), 1)) / F.count("*"), 1).alias("pct_agent_team_not_in_vip_onboar"),
        F.count(F.when(F.col("channel").isin("phone", "web"), 1)).alias("n_channel_in_phone_web"),
    )
)

result = (
    per_group
    .select(
        "priority",
        "next3_csat",
        "prev2_csat",
        "n_channel_in_phone_web",
    )
    .unpivot(
        ids=["priority", "next3_csat"],
        values=["prev2_csat", "n_channel_in_phone_web"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
