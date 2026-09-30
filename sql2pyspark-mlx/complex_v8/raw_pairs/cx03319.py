from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_all_by_closed_at_week = Window.orderBy(F.col("closed_at_week").desc(), "id")

recent = (
    df_tickets
    .filter((F.col("csat") < 5) & (F.col("agent_id") <= 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        F.date_trunc("week", F.col("closed_at")).alias("closed_at_week"),
    )
)

filtered = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isin("Onboarding", "VIP")) & (F.col("su.hired_on") < F.lit("2025-02-27").cast("date")), "left_anti")
)

with_trends = (
    filtered
    .select(
        "*",
        F.round(F.avg("requester_id").over(w_agent_id_priority), 2).alias("agent_id_priority_avg_requester_id"),
        F.sum("requester_id").over(w_agent_id_priority).alias("agent_id_priority_sum_requester_id"),
        F.round((F.unix_timestamp("closed_at_week") - F.unix_timestamp(F.lag("closed_at_week").over(w_all_by_closed_at_week))) / 60, 1).alias("minutes_since_prev_closed_at_week"),
    )
)

summary = (
    with_trends
    .groupBy(F.date_trunc("month", "closed_at").alias("closed_at_month"))
    .agg(
        F.percentile_approx("agent_id_priority_avg_requester_id", 0.5).alias("median_agent_id_priority_avg_requester_id"),
        F.min("opened_at").alias("first_opened_at"),
    )
)

result = (
    summary
    .select(
        "closed_at_month",
        "first_opened_at",
        F.date_trunc("day", F.col("closed_at_month")).alias("closed_at_month_day"),
    )
    .orderBy(F.col("first_opened_at").desc_nulls_first(), "closed_at_month")
    .limit(5)
)
