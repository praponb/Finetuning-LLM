from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(7, 11))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("csat").alias("min_ticket_csat"),
        F.count("*").alias("ticket_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.min_ticket_csat",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_closed_at",
    )
)

derived = (
    summarized_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "min_ticket_csat",
        "last_ticket_closed_at",
        F.date_format(F.col("last_ticket_closed_at"), "yyyy-MM-dd HH:00").alias("last_ticket_closed_at_label"),
        F.least(F.col("closed_at_dayofweek"), F.col("min_ticket_csat")).alias("least_closed_at_dayofweek_min_ticket_csat"),
        F.when(F.col("ticket_count") >= 6, "HIGH").when(F.col("ticket_count") >= 2, "MEDIUM").otherwise("LOW").alias("ticket_count_tier"),
    )
)

with_activity = (
    derived.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") < 3), "left_anti")
)

filtered = (
    with_activity
    .crossJoin(with_activity.agg(F.max("csat").alias("overall_max_csat")))
    .filter(F.col("csat") < F.col("overall_max_csat"))
    .drop("overall_max_csat")
)

with_activity_2 = (
    filtered
    .crossJoin(filtered.agg(F.max("requester_id").alias("overall_max_requester_id")))
    .filter(F.col("requester_id") < F.col("overall_max_requester_id"))
    .drop("overall_max_requester_id")
)

result = (
    with_activity_2
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "min_ticket_csat",
        "last_ticket_closed_at_label",
        "least_closed_at_dayofweek_min_ticket_csat",
        "ticket_count_tier",
    )
    .orderBy("min_ticket_csat")
)
