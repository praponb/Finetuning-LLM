from pyspark.sql import functions as F

recent_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "refund")) & (F.col("opened_at") >= F.lit("2025-01-07 13:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

grouped = (
    recent_tickets
    .groupBy("agent_id", "csat")
    .agg(
        F.sum("requester_id").alias("total_requester_id"),
        F.round(F.sum("csat") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("csat_to_requester_id_ratio"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.max("csat").alias("max_csat_check"),
    )
    .filter(F.col("max_csat_check") > 1)
    .drop("max_csat_check")
)

detail_rows = (
    grouped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") != "phone"), "left")
    .select(
        "s.agent_id",
        "s.csat",
        "s.csat_to_requester_id_ratio",
        "s.distinct_channels",
        F.col("t.id").alias("ticket_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.count(F.when(~F.col("channel").isin("phone", "email"), 1)).alias("ticket_channel_count"),
        F.count("*").alias("ticket_count_2"),
    )
)

summarized_rows = (
    detail_rows.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
    )
)

result = (
    summarized_rows
    .select(
        "agent_id",
        "csat",
        "csat_to_requester_id_ratio",
        "distinct_channels",
        "ticket_id",
        "ticket_channel",
        "ticket_requester_id",
        "ticket_channel_count",
        "ticket_count_2",
        F.upper(F.trim(F.col("ticket_channel"))).alias("ticket_channel_clean"),
    )
)
