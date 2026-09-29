from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.col("csat") < 3)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
    )
)

totals = (
    scoped_tickets
    .groupBy(F.date_format("opened_at", "yyyy-MM").alias("opened_at_ym"))
    .agg(
        F.sum(F.when(F.col("channel") == "chat", 1).otherwise(0)).alias("n_channel_chat"),
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.round(100 * F.count(F.when(F.col("priority") == "P2", 1)) / F.count("*"), 1).alias("pct_priority_p2"),
        F.max("opened_at").alias("last_opened_at"),
        F.round(F.sum("csat") / F.nullif(F.sum("minutes_closed_at_to_opened_at"), F.lit(0)), 3).alias("csat_to_minutes_closed_at_to_opened_at_ratio"),
        F.sum("minutes_closed_at_to_opened_at").alias("total_minutes_closed_at_to_opened_at"),
    )
)

scored = (
    totals
    .select(
        "*",
        F.upper(F.trim(F.col("opened_at_ym"))).alias("opened_at_ym_clean"),
        (F.col("pct_priority_p2") + F.col("n_channel_chat")).alias("pct_priority_p2_plus_n_channel_chat"),
    )
)

tagged = (
    scored
    .filter(F.month(F.col("last_opened_at")).isin(1, 2))
    .withColumn("last_opened_at_label", F.date_format(F.col("last_opened_at"), "MMM yyyy"))
)

per_label = (
    tagged
    .select(
        "opened_at_ym",
        "n_channel_chat",
        "pct_priority_p2",
        "last_opened_at",
        "csat_to_minutes_closed_at_to_opened_at_ratio",
        "total_minutes_closed_at_to_opened_at",
        "opened_at_ym_clean",
        "pct_priority_p2_plus_n_channel_chat",
        "last_opened_at_label",
        F.posexplode("channel_list").alias("channel_list_item_pos", "channel_list_item"),
    )
)

result = (
    per_label
    .select(
        "opened_at_ym",
        "n_channel_chat",
        "pct_priority_p2",
        "last_opened_at",
        "csat_to_minutes_closed_at_to_opened_at_ratio",
        "total_minutes_closed_at_to_opened_at",
        "pct_priority_p2_plus_n_channel_chat",
        "last_opened_at_label",
        "channel_list_item_pos",
        "channel_list_item",
        F.abs(F.col("n_channel_chat") - 5).alias("n_channel_chat_distance"),
    )
    .orderBy(
        F.col("n_channel_chat_distance").desc(),
        F.col("pct_priority_p2").desc(),
    )
)
