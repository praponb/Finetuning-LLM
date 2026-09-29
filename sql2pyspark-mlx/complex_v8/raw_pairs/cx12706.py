from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter((F.col("requester_id").between(2, 25)) & (~F.array_contains(F.col("tags"), "login")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
        F.when(F.col("channel").isin("phone", "chat"), "group_a").when(F.col("channel") == "email", "group_b").otherwise("other").alias("channel_group"),
        F.abs(F.col("requester_id") - 13).alias("requester_id_distance"),
    )
)

channel_group_stats = (
    filtered
    .groupBy("channel_group")
    .agg(F.avg("requester_id_distance").alias("avg_requester_id_distance"))
)

compared = (
    filtered.alias("s")
    .join(channel_group_stats.alias("g"), F.col("g.channel_group") == F.col("s.channel_group"), "inner")
    .filter(F.col("s.requester_id_distance") > F.col("g.avg_requester_id_distance"))
    .select(
        "s.*",
        "g.avg_requester_id_distance",
        F.round(F.col("s.requester_id_distance") - F.col("g.avg_requester_id_distance"), 2).alias("requester_id_distance_minus_avg"),
    )
)

filtered_2 = (
    compared.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.hour(F.col("t.closed_at")).between(8, 11)), "left_anti")
)

result = (
    filtered_2
    .groupingSets(
        [["csat", "channel"], ["csat"], ["channel"], []],
        "csat", "channel",
    )
    .agg(F.countDistinct("channel_group").alias("distinct_channel_groups"))
    .select(
        "csat",
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        "distinct_channel_groups",
    )
    .orderBy(F.col("csat").asc_nulls_last(), F.col("channel").asc_nulls_last())
)
