from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_agent_id_by_requester_id_to = Window.partitionBy("agent_id").orderBy("requester_id_to_csat_ratio", "agent_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_requester_id_to_2 = Window.partitionBy("agent_id").orderBy("requester_id_to_csat_ratio", "agent_id")
w_agent_id_by_second_total_req = Window.partitionBy("agent_id").orderBy("second_total_requester_id", "agent_id")
w_agent_id_n_priority_p2 = Window.partitionBy("agent_id", "n_priority_p2")
w_agent_id_by_second_total_req_2 = Window.partitionBy("agent_id").orderBy("second_total_requester_id", "agent_id").rowsBetween(-1, -1)
w_agent_id_by_second_total_req_3 = Window.partitionBy("agent_id").orderBy("second_total_requester_id", "agent_id").rowsBetween(-6, Window.currentRow)

recent_tickets = (
    df_tickets
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

grouped = (
    recent_tickets
    .groupBy("agent_id")
    .agg(
        F.round(F.sum("requester_id") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("requester_id_to_csat_ratio"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.sum("requester_id").alias("total_requester_id"),
        F.sum(F.when(F.col("priority") == "P2", 1).otherwise(0)).alias("n_priority_p2"),
    )
)

ranked = (
    grouped
    .select(
        "*",
        F.round(F.col("requester_id_to_csat_ratio") - F.avg("requester_id_to_csat_ratio").over(w_agent_id), 2).alias("requester_id_to_csat_ratio_vs_agent_id_avg"),
        F.round(F.avg("n_priority_p2").over(w_agent_id), 2).alias("agent_id_avg_n_priority_p2"),
        F.nth_value("total_requester_id", 2).over(w_agent_id_by_requester_id_to).alias("second_total_requester_id"),
        F.lag("requester_id_to_csat_ratio").over(w_agent_id_by_requester_id_to_2).alias("prev_requester_id_to_csat_ratio"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
    )
)

with_activity = (
    ranked.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.priority").isNotNull()) & (F.col("t.closed_at") >= F.lit("2025-02-08 13:00:00").cast("timestamp")), "left_semi")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.sum("requester_id").alias("total_ticket_requester_id"),
        F.sum("requester_id").alias("total_ticket_requester_id_2"),
    )
)

profiled = (
    with_activity.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.total_ticket_requester_id", "t.total_ticket_requester_id_2")
)

metrics = (
    profiled
    .select(
        "*",
        F.ntile(5).over(w_agent_id_by_second_total_req).alias("second_total_requester_id_quintile"),
        F.round(F.col("n_priority_p2") - F.avg("n_priority_p2").over(w_agent_id_n_priority_p2), 2).alias("n_priority_p2_vs_agent_id_n_priority_p2_avg"),
        F.round(F.avg("total_ticket_requester_id").over(w_agent_id_by_second_total_req_2), 2).alias("total_ticket_requester_id_avg_1_prior"),
        F.sum("total_ticket_requester_id_2").over(w_agent_id_by_second_total_req_3).alias("total_ticket_requester_id_2_sum_7"),
    )
)

qualified = (
    metrics.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-02-26").cast("date"))) & (~F.col("su.team").isin("Tech", "Onboarding")), "left_semi")
)

result = (
    qualified
    .select(
        "agent_id",
        "requester_id_to_csat_ratio",
        "n_priority_p2",
        "requester_id_to_csat_ratio_vs_agent_id_avg",
        "agent_id_avg_n_priority_p2",
        "second_total_requester_id",
        "rows_in_agent_id",
        "second_total_requester_id_quintile",
        "n_priority_p2_vs_agent_id_n_priority_p2_avg",
        "total_ticket_requester_id_2_sum_7",
        (F.col("total_requester_id") + F.col("distinct_channels")).alias("total_requester_id_plus_distinct_channels"),
    )
    .orderBy(
        F.col("total_requester_id_plus_distinct_channels").desc_nulls_first(),
        "n_priority_p2_vs_agent_id_n_priority_p2_avg",
        "agent_id",
    )
    .limit(20)
)
