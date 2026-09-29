from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "outage"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("agent_id").cast("string")).alias("channel_ref"),
    )
)

agent_id_stats = (
    recent
    .groupBy("agent_id")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

outliers = (
    recent.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

screened = (
    outliers.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.requester_id") < 16), "left_anti")
)

derived = (
    screened
    .filter(F.col("requester_id_minus_avg") > -7.5)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "tags",
        "avg_requester_id",
        "requester_id_minus_avg",
        F.abs(F.col("requester_id_minus_avg") - 10.0).alias("requester_id_minus_avg_distance"),
        F.when(F.col("channel").isin("chat", "web"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
        (F.col("requester_id_minus_avg") + F.col("avg_requester_id")).alias("requester_id_minus_avg_plus_avg_requester_id"),
    )
)

result = (
    derived
    .filter(F.col("channel_group").isin("group_b", "other"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "tags",
        "avg_requester_id",
        "requester_id_minus_avg",
        "requester_id_minus_avg_distance",
        "channel_group",
        "requester_id_minus_avg_plus_avg_requester_id",
        (F.col("requester_id_minus_avg") + F.col("avg_requester_id")).alias("requester_id_minus_avg_plus_avg_requester_id_2"),
        F.least(F.col("avg_requester_id"), F.col("requester_id_minus_avg")).alias("least_avg_requester_id_requester_id_minus_avg"),
    )
    .orderBy(F.col("requester_id_minus_avg_distance").asc_nulls_last())
)
