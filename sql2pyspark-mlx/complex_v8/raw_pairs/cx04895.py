from pyspark.sql import functions as F

base = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "outage")) & (F.col("agent_id") <= 7))
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
        F.month(F.col("opened_at")).alias("opened_at_month"),
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
    )
)

scored = (
    base
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
        "greatest_requester_id_csat",
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.min("requester_id").alias("min_ticket_requester_id"),
    )
)

profiled = (
    scored.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.first_ticket_closed_at",
        "t.min_ticket_requester_id",
    )
)

annotated = (
    profiled
    .filter(F.col("requester_id") > 2)
    .select(
        "*",
        F.least(F.col("requester_id"), F.col("min_ticket_requester_id")).alias("least_requester_id_min_ticket_requester_id"),
        F.when(F.col("channel").isin("web", "email"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

result = (
    annotated
    .select(
        "id",
        "requester_id",
        "greatest_requester_id_csat",
        "least_requester_id_min_ticket_requester_id",
    )
    .unpivot(
        ids=["id"],
        values=["requester_id", "greatest_requester_id_csat", "least_requester_id_min_ticket_requester_id"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric_name")
)
