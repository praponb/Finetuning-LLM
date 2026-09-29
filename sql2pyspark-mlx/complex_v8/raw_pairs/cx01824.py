from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter((F.col("opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.col("requester_id").between(7, 17)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
    )
)

enriched = (
    base_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.tags",
        "s.csat_per_requester_id",
        "s.priority_or_unknown",
        "s.greatest_requester_id_csat",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

result = (
    enriched
    .groupingSets(
        [["agent_id", "priority_or_unknown"], ["agent_id"], []],
        "agent_id", "priority_or_unknown",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("channel") != "phone", 1)) / F.count("*"), 1).alias("pct_channel_phone"),
        F.sum("greatest_requester_id_csat").alias("total_greatest_requester_id_csat"),
        F.count(F.when(F.col("channel") == "email", 1)).alias("n_channel_email"),
    )
    .select(
        "agent_id",
        F.coalesce(F.col("priority_or_unknown"), F.lit("ALL")).alias("priority_or_unknown"),
        "pct_channel_phone",
        "total_greatest_requester_id_csat",
        "n_channel_email",
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("priority_or_unknown").asc_nulls_last(),
    )
)
