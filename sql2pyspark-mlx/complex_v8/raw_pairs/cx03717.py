from pyspark.sql import functions as F

base = (
    df_tickets
    .filter((F.col("agent_id") <= 9) & (F.col("csat") <= 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("requester_id").cast("string")).alias("priority_ref"),
    )
)

with_details = (
    base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.year(F.col("su.hired_on")) == 2025)
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.min("requester_id").alias("min_ticket_requester_id"),
        F.max("opened_at").alias("last_ticket_opened_at"),
    )
)

profiled = (
    with_details.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.min_ticket_requester_id",
        "t.last_ticket_opened_at",
    )
)

result = (
    profiled
    .cube("ticket_count", "agent_id", "channel")
    .agg(
        F.min("requester_id").alias("min_requester_id"),
        F.count("*").alias("row_count"),
        F.max("min_ticket_requester_id").alias("max_min_ticket_requester_id"),
    )
    .select(
        "ticket_count",
        "agent_id",
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        "min_requester_id",
        "row_count",
        "max_min_ticket_requester_id",
    )
    .orderBy(
        F.col("ticket_count").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("channel").asc_nulls_last(),
    )
)
