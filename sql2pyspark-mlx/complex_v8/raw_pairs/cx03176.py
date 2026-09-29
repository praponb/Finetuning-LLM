from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

combined_details = (
    matched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.agent_id",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

enriched_agent = (
    combined_details.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.agent_id",
        "s.ticket_tags",
        "s.ticket_channel",
        "s.agent_hired_on",
        "s.agent_team",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on_2"),
    )
)

totals = (
    enriched_agent
    .groupBy("agent_team")
    .agg(
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("agent_name"))) >= 5, 1)) / F.count("*"), 1).alias("pct_length_trim_agent_name_5"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.sum(F.when(F.lower(F.col("agent_name")).like("%ak%"), 1).otherwise(0)).alias("n_lower_agent_name_like_ak"),
        F.datediff(F.max("agent_hired_on"), F.min("agent_hired_on")).alias("agent_hired_on_span_days"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("agent_hired_on").alias("last_agent_hired_on"),
    )
)

result = (
    totals
    .select(
        "agent_team",
        "pct_length_trim_agent_name_5",
        "distinct_agent_ids",
        "n_lower_agent_name_like_ak",
        "agent_hired_on_span_days",
        "last_agent_hired_on",
    )
    .orderBy(
        F.col("pct_length_trim_agent_name_5").desc(),
        F.col("last_agent_hired_on").asc_nulls_last(),
    )
)
