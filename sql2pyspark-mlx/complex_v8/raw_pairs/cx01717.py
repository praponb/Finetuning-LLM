from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        (F.col("opened_at") + F.expr("INTERVAL 2 HOURS")).alias("opened_at_plus_2h"),
        F.date_format(F.col("opened_at"), "yyyy-MM-dd").alias("opened_at_label"),
    )
)

without_matches = (
    filtered_tickets
    .crossJoin(filtered_tickets.agg(F.avg("csat").alias("overall_avg_csat")))
    .filter(F.col("csat") > F.col("overall_avg_csat"))
    .drop("overall_avg_csat")
)

channel_stats = (
    without_matches
    .groupBy("channel")
    .agg(F.avg("csat").alias("avg_csat"))
)

outliers = (
    without_matches.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

combined_details = (
    outliers.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select("s.*", "su.agent_name")
)

result = (
    combined_details
    .groupingSets(
        [["agent_id", "priority", "csat"], ["agent_id"], []],
        "agent_id", "priority", "csat",
    )
    .agg(
        F.percentile_approx("avg_csat", 0.5).alias("median_avg_csat"),
        F.sum(F.when(F.upper(F.trim(F.col("agent_name"))).like("H%"), F.col("csat_minus_avg"))).alias("csat_minus_avg_upper_trim_agent_name_like_h"),
    )
)
