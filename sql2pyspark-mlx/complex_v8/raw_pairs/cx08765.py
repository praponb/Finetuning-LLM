from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "tags",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("closed_at").alias("last_ticket_closed_at"),
        F.count("*").alias("ticket_count"),
    )
)

profiled = (
    prepared.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.last_ticket_closed_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

flattened = (
    profiled
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "last_ticket_closed_at",
        "ticket_count",
        F.explode_outer("tags").alias("tag"),
    )
)

scored = (
    flattened
    .filter(F.col("agent_id") <= 8)
    .select(
        "id",
        "agent_id",
        "ticket_count",
        "tag",
        F.dayofweek(F.col("last_ticket_closed_at")).alias("last_ticket_closed_at_dayofweek"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.date_trunc("year", F.col("last_ticket_closed_at")).alias("last_ticket_closed_at_year"),
    )
)

result = (
    scored
    .select(
        "id",
        "agent_id",
        "ticket_count",
        "tag",
        "last_ticket_closed_at_dayofweek",
        "priority_or_unknown",
        "last_ticket_closed_at_year",
        F.coalesce(F.col("ticket_count"), F.lit(0)).alias("ticket_count_filled"),
    )
    .orderBy(
        F.col("ticket_count_filled").asc_nulls_last(),
        F.col("ticket_count").desc_nulls_first(),
    )
)
