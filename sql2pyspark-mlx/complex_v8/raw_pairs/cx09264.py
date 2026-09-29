from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter(F.col("channel") == "email")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

agent_id_stats = (
    prepared_tickets
    .groupBy("agent_id")
    .agg(F.avg("channel_len").alias("avg_channel_len"))
)

vs_group = (
    prepared_tickets.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.channel_len") > F.col("g.avg_channel_len"))
    .select(
        "s.*",
        "g.avg_channel_len",
        F.round(F.col("s.channel_len") - F.col("g.avg_channel_len"), 2).alias("channel_len_minus_avg"),
    )
)

result = (
    vs_group
    .select("id", "channel_len_minus_avg", "avg_channel_len")
    .unpivot(
        ids=["id"],
        values=["channel_len_minus_avg", "avg_channel_len"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
