from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter((F.col("channel").isin("phone", "chat", "web")) & (F.size(F.col("tags")) >= 1))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
    )
)

channel_stats = (
    valid_tickets
    .groupBy("channel")
    .agg(F.avg("csat_plus_requester_id").alias("avg_csat_plus_requester_id"))
)

vs_group = (
    valid_tickets.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.csat_plus_requester_id") > F.col("g.avg_csat_plus_requester_id"))
    .select(
        "s.*",
        "g.avg_csat_plus_requester_id",
        F.round(F.col("s.csat_plus_requester_id") - F.col("g.avg_csat_plus_requester_id"), 2).alias("csat_plus_requester_id_minus_avg"),
    )
)

with_activity = (
    vs_group
    .crossJoin(vs_group.agg(F.max("avg_csat_plus_requester_id").alias("overall_max_avg_csat_plus_requester_id")))
    .filter(F.col("avg_csat_plus_requester_id") < F.col("overall_max_avg_csat_plus_requester_id"))
    .drop("overall_max_avg_csat_plus_requester_id")
)

result = (
    with_activity
    .groupingSets(
        [["agent_id", "priority"], ["agent_id"], []],
        "agent_id", "priority",
    )
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.round(100 * F.count(F.when(F.col("priority") != "P3", 1)) / F.count("*"), 1).alias("pct_priority_p3"),
    )
    .orderBy(F.col("agent_id").asc_nulls_last(), F.col("priority").asc_nulls_last())
)
