from pyspark.sql import functions as F

scoped_agents = (
    df_support_agents
    .filter(F.col("team") == "Tech")
    .select(
        "id",
        "team",
        "tier",
        "hired_on",
        F.substring(F.trim(F.col("agent_name")), 1, 2).alias("agent_name_prefix"),
        F.when(F.col("tier").isin("T1", "T3"), "group_a").when(F.col("tier") == "T2", "group_b").otherwise("other").alias("tier_group"),
    )
)

detail_rows = (
    scoped_agents.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.hired_on",
        "s.tier_group",
        F.col("t.id").alias("ticket_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

without_matches = (
    detail_rows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.size(F.col("t.tags")) >= 1), "left_semi")
    .filter(F.col("hired_on") >= F.lit("2024-07-09").cast("date"))
)

enriched = (
    without_matches
    .select(
        "*",
        F.concat_ws("-", F.col("ticket_channel"), F.col("tier_group")).alias("ticket_channel_tier_group_label"),
    )
)

result = (
    enriched
    .select(
        "id",
        "hired_on",
        "tier_group",
        "ticket_id",
        "ticket_tags",
        "ticket_channel",
        "ticket_closed_at",
        "ticket_channel_tier_group_label",
        F.to_date(F.col("ticket_closed_at")).alias("ticket_closed_at_date"),
        F.date_trunc("month", F.col("ticket_closed_at")).alias("ticket_closed_at_month"),
    )
)
