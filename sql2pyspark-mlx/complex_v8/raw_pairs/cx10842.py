from pyspark.sql import functions as F

joined_ranges = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.requester_id").alias("prior_requester_id_12h"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("opened_at").alias("first_ticket_opened_at"),
        F.count(F.when(F.col("channel") != "email", 1)).alias("ticket_channel_count"),
        F.sum("requester_id").alias("total_ticket_requester_id"),
    )
)

summarized_rows = (
    joined_ranges.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.first_ticket_opened_at",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        "t.total_ticket_requester_id",
    )
)

labeled = (
    summarized_rows
    .filter(F.col("prior_rows_12h") <= 4)
    .withColumn("minutes_first_ticket_opened_at_to_opened_at", F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("first_ticket_opened_at"))) / 60, 1))
)

result = (
    labeled
    .filter(F.to_date(F.col("opened_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "agent_id",
        "prior_rows_12h",
        "prior_requester_id_12h",
        "first_ticket_opened_at",
        "ticket_channel_count",
        "total_ticket_requester_id",
        "minutes_first_ticket_opened_at_to_opened_at",
        F.round((F.unix_timestamp(F.col("first_ticket_opened_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_first_ticket_opened_at"),
    )
)
