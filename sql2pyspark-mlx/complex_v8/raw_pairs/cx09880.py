from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter(F.col("agent_id") <= 5)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
    )
)

enriched = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.requester_id_per_csat",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

joined_agent = (
    enriched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.tier").isin("T3", "T2"))
    .select(
        "s.id",
        "s.agent_id",
        "s.csat",
        "s.agent_hired_on",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

annotated = joined_agent.select("*", F.length(F.trim(F.col("agent_tier"))).alias("agent_tier_len"))

joined_agent_2 = (
    annotated.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.csat",
        "s.agent_hired_on",
        "s.agent_tier",
        "s.agent_tier_len",
        F.col("su.tier").alias("agent_tier_2"),
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

labeled = (
    joined_agent_2
    .select(
        "id",
        "csat",
        "agent_hired_on",
        "agent_tier",
        "agent_tier_len",
        "agent_agent_name",
        F.upper(F.trim(F.col("agent_agent_name"))).alias("agent_agent_name_clean"),
    )
)

result = (
    labeled
    .cube("agent_tier", "csat")
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("agent_agent_name"))).like("J%"), F.col("csat"))).alias("csat_upper_trim_agent_agent_name"),
        F.max("csat").alias("max_csat"),
    )
    .orderBy(F.col("agent_tier").asc_nulls_last(), F.col("csat").asc_nulls_last())
)
