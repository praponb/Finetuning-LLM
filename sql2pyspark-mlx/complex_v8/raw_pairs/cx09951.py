from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat_minus_requester = Window.orderBy(F.col("csat_minus_requester_id").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_channel_by_csat_minus_reques = Window.partitionBy("channel").orderBy("csat_minus_requester_id", "id").rowsBetween(-3, -1)

scoped = (
    df_tickets
    .filter(F.col("priority").isin("P3", "P1"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.sum("requester_id").over(w_all_by_csat_minus_requester).alias("requester_id_sum_3_prior"),
        F.max("csat_minus_requester_id").over(w_agent_id_channel).alias("agent_id_channel_max_csat_minus_requester_id"),
        F.sum("csat_minus_requester_id").over(w_channel_by_csat_minus_reques).alias("csat_minus_requester_id_sum_3_prior"),
    )
)

with_details = (
    sequenced.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") == "Onboarding")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.tags",
        "s.csat_minus_requester_id",
        "s.agent_id_channel_max_csat_minus_requester_id",
        "s.csat_minus_requester_id_sum_3_prior",
        F.col("su.team").alias("agent_team"),
    )
)

with_activity = (
    with_details.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isNotNull()) & (F.upper(F.trim(F.col("su.agent_name"))).like("K%")), "left_semi")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.min("requester_id").alias("min_ticket_requester_id"))
)

summarized_rows = (
    with_activity.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.min_ticket_requester_id")
)

combined_details_agent = (
    summarized_rows.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").isNotNull()), "inner")
    .filter(F.col("su.hired_on").isNotNull())
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.tags",
        "s.csat_minus_requester_id",
        "s.agent_id_channel_max_csat_minus_requester_id",
        "s.csat_minus_requester_id_sum_3_prior",
        "s.agent_team",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

result = (
    combined_details_agent
    .groupingSets(
        [["agent_tier", "agent_id", "agent_team"], ["agent_tier"], []],
        "agent_tier", "agent_id", "agent_team",
    )
    .agg(
        F.round(F.sum("agent_id_channel_max_csat_minus_requester_id") / F.nullif(F.sum("csat_minus_requester_id_sum_3_prior"), F.lit(0)), 3).alias("agent_id_channel_max_csat_minus_requester_id_to_"),
    )
    .orderBy(
        F.col("agent_tier").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("agent_team").asc_nulls_last(),
    )
)
