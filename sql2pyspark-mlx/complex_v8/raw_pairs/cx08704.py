from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_all_by_requester_id_ties = Window.orderBy("requester_id")

base_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
    )
)

qualified = (
    base_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.size(F.col("t.tags")) >= 2) & (F.col("t.opened_at") >= F.lit("2025-01-07 13:00:00").cast("timestamp")), "left_anti")
)

with_windows = (
    qualified
    .select(
        "*",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_priority_agent_id), 2).alias("requester_id_vs_priority_agent_id_avg"),
        (F.col("requester_id") - F.lag("requester_id").over(w_all_by_requester_id)).alias("requester_id_change"),
        F.round(F.percent_rank().over(w_all_by_requester_id_ties), 3).alias("requester_id_percent_rank"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_priority_agent_id), F.lit(0)), 4).alias("requester_id_share_of_priority_agent_id"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.count("*").alias("ticket_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

with_counts = (
    with_windows.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.first_ticket_closed_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_closed_at",
    )
)

derived = with_counts.withColumn("priority_len", F.length(F.trim(F.col("priority"))))

result = (
    derived
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "tags",
        "requester_id_vs_priority_agent_id_avg",
        "requester_id_change",
        "requester_id_percent_rank",
        "requester_id_share_of_priority_agent_id",
        "first_ticket_closed_at",
        "ticket_count",
        "last_ticket_closed_at",
        "priority_len",
        F.when(F.col("priority").isin("P2", "P1"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
    )
    .orderBy(
        F.col("first_ticket_closed_at").desc_nulls_first(),
        F.col("requester_id_share_of_priority_agent_id").desc_nulls_first(),
    )
)
