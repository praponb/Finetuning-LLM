from pyspark.sql import functions as F

filtered_agents = (
    df_support_agents
    .filter((F.col("team").isNotNull()) & (F.col("agent_name").isNotNull()))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.date_format(F.col("hired_on"), "yyyy-MM").alias("hired_on_label"),
        F.add_months(F.col("hired_on"), 12).alias("hired_on_plus_12m"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("requester_id").alias("max_ticket_requester_id"),
        F.count("*").alias("ticket_count"),
        F.count("*").alias("ticket_count_2"),
    )
)

with_stats = (
    filtered_agents.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.max_ticket_requester_id",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
    )
)

labeled = (
    with_stats
    .filter(F.col("ticket_count") >= 1)
    .select(
        "*",
        F.least(F.col("ticket_count_2"), F.col("ticket_count")).alias("least_ticket_count_2_ticket_count"),
        F.coalesce(F.col("tier"), F.lit("UNKNOWN")).alias("tier_or_unknown"),
    )
)

annotated = (
    labeled
    .select(
        "*",
        F.abs(F.col("ticket_count_2") - 3).alias("ticket_count_2_distance"),
        F.date_format(F.col("hired_on_plus_12m"), "yyyy-MM").alias("hired_on_plus_12m_label"),
        F.greatest(F.col("ticket_count"), F.col("ticket_count_2")).alias("greatest_ticket_count_ticket_count_2"),
    )
)

result = (
    annotated
    .cube("ticket_count_2_distance", "tier", "ticket_count")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("tier").alias("distinct_tiers"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        "ticket_count_2_distance",
        F.coalesce(F.col("tier"), F.lit("ALL")).alias("tier"),
        "ticket_count",
        "n_rows",
        "distinct_tiers",
        "distinct_ids",
    )
    .orderBy(
        F.col("ticket_count_2_distance").asc_nulls_last(),
        F.col("tier").asc_nulls_last(),
        F.col("ticket_count").asc_nulls_last(),
    )
)
