from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tier_team_by_hired_on = Window.partitionBy("tier", "team").orderBy(F.col("hired_on").desc(), "id")
w_all_by_hired_on_full = Window.orderBy(F.col("hired_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_hired_on_ties = Window.orderBy(F.col("hired_on").asc_nulls_last())

recent = (
    df_support_agents
    .filter((F.col("team") == "Tech") & (F.col("agent_name").isNotNull()))
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.when(F.col("tier").isin("T3", "T2"), "group_a").when(F.col("tier") == "T1", "group_b").otherwise("other").alias("tier_group"),
    )
)

windowed = (
    recent
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        "tier_group",
        F.lag("hired_on", 3).over(w_tier_team_by_hired_on).alias("prev3_hired_on"),
        F.nth_value("hired_on", 2).over(w_all_by_hired_on_full).alias("second_hired_on"),
        F.round(F.cume_dist().over(w_all_by_hired_on_ties), 3).alias("hired_on_cume_dist"),
        F.last("hired_on").over(w_all_by_hired_on_full).alias("last_hired_on"),
        F.round(F.percent_rank().over(w_all_by_hired_on_ties), 3).alias("hired_on_percent_rank"),
    )
)

with_children = (
    windowed.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.closed_at") >= F.lit("2025-01-14 03:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.team",
        "s.tier",
        "s.tier_group",
        "s.prev3_hired_on",
        "s.second_hired_on",
        "s.hired_on_percent_rank",
        F.col("t.id").alias("ticket_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

enriched_ticket = (
    with_children.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.tier",
        "s.tier_group",
        "s.prev3_hired_on",
        "s.second_hired_on",
        "s.hired_on_percent_rank",
        "s.ticket_id",
        "s.ticket_csat",
        "s.ticket_priority",
        "s.ticket_channel",
        "s.ticket_closed_at",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

result = (
    enriched_ticket
    .groupingSets(
        [["ticket_priority", "ticket_agent_id", "ticket_channel"], ["ticket_priority"], ["ticket_agent_id"]],
        "ticket_priority", "ticket_agent_id", "ticket_channel",
    )
    .agg(
        F.round(F.sum("ticket_requester_id") / F.nullif(F.sum("hired_on_percent_rank"), F.lit(0)), 3).alias("ticket_requester_id_to_hired_on_percent_rank_rat"),
        F.round(F.sum("hired_on_percent_rank") / F.nullif(F.sum("ticket_requester_id"), F.lit(0)), 3).alias("hired_on_percent_rank_to_ticket_requester_id_rat"),
    )
    .select(
        F.coalesce(F.col("ticket_priority"), F.lit("ALL")).alias("ticket_priority"),
        "ticket_agent_id",
        F.coalesce(F.col("ticket_channel"), F.lit("ALL")).alias("ticket_channel"),
        "ticket_requester_id_to_hired_on_percent_rank_rat",
        "hired_on_percent_rank_to_ticket_requester_id_rat",
    )
)
