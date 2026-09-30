from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_priority_by_csat_per_request = Window.partitionBy("priority").orderBy("csat_per_requester_id", "id")
w_priority_by_csat_per_request_2 = Window.partitionBy("priority").orderBy("csat_per_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_by_csat_per_request_3 = Window.partitionBy("priority").orderBy("csat_per_requester_id", "id").rowsBetween(-2, Window.currentRow)
w_priority_by_csat_per_request_4 = Window.partitionBy("priority").orderBy("csat_per_requester_id")

cleaned = (
    df_tickets
    .filter((F.col("requester_id") <= 21) & (F.col("channel") == "web"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.max("csat_minus_requester_id").over(w_priority_agent_id).alias("priority_agent_id_max_csat_minus_requester_id"),
        F.ntile(5).over(w_priority_by_csat_per_request).alias("csat_per_requester_id_quintile"),
        F.round(F.cume_dist().over(w_priority_by_csat_per_request), 3).alias("csat_per_requester_id_cume_dist"),
        F.nth_value("csat_per_requester_id", 2).over(w_priority_by_csat_per_request_2).alias("second_csat_per_requester_id"),
        F.max("csat_per_requester_id").over(w_priority_by_csat_per_request_3).alias("csat_per_requester_id_max_3"),
        F.round(F.percent_rank().over(w_priority_by_csat_per_request_4), 3).alias("csat_per_requester_id_percent_rank"),
    )
)

joined_agent = (
    sequenced.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat_minus_requester_id",
        "s.csat_per_requester_id",
        "s.priority_agent_id_max_csat_minus_requester_id",
        "s.csat_per_requester_id_quintile",
        "s.csat_per_requester_id_cume_dist",
        "s.csat_per_requester_id_max_3",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.count("*").alias("ticket_count_2"),
    )
)

profiled = (
    joined_agent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.first_ticket_closed_at",
        F.coalesce(F.col("t.ticket_count_2"), F.lit(0)).alias("ticket_count_2"),
    )
)

stats = (
    profiled
    .groupBy(F.to_date("closed_at").alias("closed_at_day"), "ticket_count_2")
    .agg(
        F.round(F.sum("priority_agent_id_max_csat_minus_requester_id") / F.nullif(F.sum("csat_per_requester_id"), F.lit(0)), 3).alias("priority_agent_id_max_csat_minus_requester_id_to"),
        F.count("*").alias("n_rows"),
    )
)

filtered = (
    stats
    .crossJoin(stats.agg(F.avg("priority_agent_id_max_csat_minus_requester_id_to").alias("overall_avg_priority_agent_id_max_csat_minus_req")))
    .filter(F.col("priority_agent_id_max_csat_minus_requester_id_to") > F.col("overall_avg_priority_agent_id_max_csat_minus_req"))
    .drop("overall_avg_priority_agent_id_max_csat_minus_req")
)

labeled = (
    filtered
    .filter(F.year(F.col("closed_at_day")) == 2025)
    .select("*", F.month(F.col("closed_at_day")).alias("closed_at_day_month"))
)

result = (
    labeled
    .select(
        "closed_at_day",
        "ticket_count_2",
        F.col("n_rows").alias("n_rows_v"),
        F.col("closed_at_day_month").alias("closed_at_day_month_v"),
    )
    .unpivot(
        ids=["closed_at_day", "ticket_count_2"],
        values=["n_rows_v", "closed_at_day_month_v"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("closed_at_day", "ticket_count_2", "measure")
)
