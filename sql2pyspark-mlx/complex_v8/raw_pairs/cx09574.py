from pyspark.sql import functions as F

scoped = (
    df_tickets
    .filter(F.col("priority").isin("P4", "P1", "P2"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "tags",
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

stats = (
    scoped
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.sum("requester_id_per_csat").alias("sum_requester_id_per_csat_check"),
    )
    .filter(F.col("sum_requester_id_per_csat_check") > 2.5)
    .drop("sum_requester_id_per_csat_check")
)

per_label = (
    stats
    .select(
        "agent_id",
        "row_count",
        "distinct_prioritys",
        "distinct_agent_ids",
        F.posexplode("priority_list").alias("priority_list_item_pos", "priority_list_item"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.count("*").alias("ticket_count"),
        F.max("opened_at").alias("last_ticket_opened_at"),
    )
)

summarized_rows = (
    per_label.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.first_ticket_closed_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_opened_at",
    )
)

result = (
    summarized_rows
    .select(
        "agent_id",
        "row_count",
        "distinct_agent_ids",
        "priority_list_item_pos",
        "priority_list_item",
        "first_ticket_closed_at",
        "ticket_count",
        "last_ticket_opened_at",
    )
    .orderBy(F.col("last_ticket_opened_at").desc(), "agent_id")
)
