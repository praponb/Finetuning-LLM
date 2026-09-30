from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.date_format(F.col("opened_at"), "yyyy-MM-dd").alias("opened_at_label"),
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
    )
)

priority_stats = (
    base_tickets
    .groupBy("priority")
    .agg(F.max("csat").alias("max_csat"))
)

compared = (
    base_tickets.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.csat") == F.col("g.max_csat"))
    .select(
        "s.*",
        "g.max_csat",
        F.round(F.col("s.csat") - F.col("g.max_csat"), 2).alias("csat_minus_max"),
    )
)

joined_agent = (
    compared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", "su.agent_name", F.col("su.team").alias("agent_team"))
)

tagged = (
    joined_agent
    .filter(F.col("priority") != "P3")
    .withColumn("opened_at_label_2", F.date_format(F.col("opened_at"), "yyyy-MM-dd"))
)

result = (
    tagged
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "opened_at_label",
        "closed_at_hour",
        "max_csat",
        "agent_name",
        F.lower(F.regexp_replace(F.trim(F.col("agent_name")), " ", "_")).alias("agent_name_slug"),
    )
)
