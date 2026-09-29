from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        F.date_format(F.col("closed_at"), "yyyy-MM-dd HH:00").alias("closed_at_label"),
        F.when(F.col("priority").isin("P3", "P1"), "group_a").when(F.col("priority") == "P4", "group_b").otherwise("other").alias("priority_group"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

priority_group_stats = (
    prepared
    .groupBy("priority_group")
    .agg(F.avg("csat").alias("avg_csat"))
)

outliers = (
    prepared.alias("s")
    .join(priority_group_stats.alias("g"), F.col("g.priority_group") == F.col("s.priority_group"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

with_details = (
    outliers.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team").isNotNull())
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.closed_at_label",
        "s.priority_group",
        "s.channel_len",
        "s.avg_csat",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_details
    .filter(F.col("avg_csat") <= 4.0)
    .select(
        "id",
        "agent_id",
        "closed_at_label",
        "priority_group",
        "channel_len",
        "avg_csat",
    )
    .orderBy(F.col("closed_at_label").asc(), F.col("avg_csat").desc())
)
