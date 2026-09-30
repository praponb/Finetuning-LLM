from pyspark.sql import functions as F

base = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 2) & (F.col("agent_id") <= 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

annotated = (
    base
    .select(
        "*",
        F.concat(F.col("channel"), F.lit("#"), F.col("agent_id").cast("string")).alias("channel_ref"),
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority").isNotNull(), 1)).alias("ticket_priority_count"),
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.count("*").alias("ticket_count"),
    )
)

with_stats = (
    annotated.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        "t.first_ticket_closed_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

joined = (
    with_stats.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.channel_ref",
        "s.greatest_csat_requester_id",
        "s.first_ticket_closed_at",
        "s.ticket_count",
        "su.agent_name",
    )
)

result = (
    joined
    .groupingSets(
        [["agent_id", "csat", "priority"], ["agent_id"], []],
        "agent_id", "csat", "priority",
    )
    .agg(
        F.max("closed_at").alias("last_closed_at"),
        F.count("*").alias("row_count"),
    )
    .select(
        "agent_id",
        "csat",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "last_closed_at",
        "row_count",
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("csat").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
    )
)
