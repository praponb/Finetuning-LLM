from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(~F.array_contains(F.col("tags"), "outage"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "tags",
        F.abs(F.col("requester_id") - 18).alias("requester_id_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

scored = (
    prepared
    .filter(F.col("agent_id") >= 9)
    .withColumn("priority_clean", F.lower(F.trim(F.col("priority"))))
)

scored_2 = (
    scored
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "requester_id_distance",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.length(F.trim(F.col("priority"))).alias("priority_len_2"),
        F.size(F.col("tags")).alias("tags_count"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("requester_id").alias("max_ticket_requester_id"),
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.sum("csat").alias("total_ticket_csat"),
    )
)

summarized_rows = (
    scored_2.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.max_ticket_requester_id",
        "t.first_ticket_closed_at",
        "t.total_ticket_csat",
    )
)

qualified = (
    summarized_rows.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") != "web"), "left_anti")
)

result = (
    qualified
    .filter(F.col("priority_len_2").between(6, 13))
    .select(
        "id",
        "agent_id",
        "priority",
        "requester_id_distance",
        "priority_len",
        "priority_len_2",
        "tags_count",
        "max_ticket_requester_id",
        "total_ticket_csat",
        F.abs(F.col("tags_count") - 1).alias("tags_count_distance"),
        (F.col("first_ticket_closed_at") + F.expr("INTERVAL 1 DAYS")).alias("first_ticket_closed_at_plus_1d"),
    )
)
