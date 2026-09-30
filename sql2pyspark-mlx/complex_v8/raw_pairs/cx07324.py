from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_agent_hired_on_month_ro = Window.partitionBy(F.date_trunc("month", "last_agent_hired_on"), "row_count")

valid_tickets = (
    df_tickets
    .filter(F.col("priority") != "P2")
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
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

with_details = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on") >= F.lit("2024-05-02").cast("date")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.days_since_opened_at",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

grouped = (
    with_details
    .groupBy(F.date_trunc("month", "agent_hired_on").alias("agent_hired_on_month"))
    .agg(
        F.sum("days_since_opened_at").alias("total_days_since_opened_at"),
        F.max("agent_hired_on").alias("last_agent_hired_on"),
        F.max("days_since_opened_at").alias("max_days_since_opened_at"),
        F.round(F.sum("csat") / F.nullif(F.sum("days_since_opened_at"), F.lit(0)), 3).alias("csat_to_days_since_opened_at_ratio"),
        F.count("*").alias("row_count"),
        F.avg("csat").alias("avg_csat_check"),
    )
    .filter(F.col("avg_csat_check") > 2)
    .drop("avg_csat_check")
)

with_activity = (
    grouped
    .crossJoin(grouped.agg(F.max("total_days_since_opened_at").alias("overall_max_total_days_since_opened_at")))
    .filter(F.col("total_days_since_opened_at") < F.col("overall_max_total_days_since_opened_at"))
    .drop("overall_max_total_days_since_opened_at")
)

labeled = (
    with_activity
    .filter(F.to_date(F.col("agent_hired_on_month")) < F.lit("2025-01-25").cast("date"))
    .select(
        "agent_hired_on_month",
        "total_days_since_opened_at",
        "last_agent_hired_on",
        "max_days_since_opened_at",
        "csat_to_days_since_opened_at_ratio",
        "row_count",
        (F.col("max_days_since_opened_at") - F.col("row_count")).alias("max_days_since_opened_at_minus_row_count"),
    )
)

ranked = (
    labeled
    .select(
        "agent_hired_on_month",
        "max_days_since_opened_at",
        "row_count",
        F.count(F.lit(1)).over(w_last_agent_hired_on_month_ro).alias("rows_in_last_agent_hired_on_month_row_count"),
        F.round(F.col("total_days_since_opened_at") - F.avg("total_days_since_opened_at").over(w_last_agent_hired_on_month_ro), 2).alias("total_days_since_opened_at_vs_last_agent_hired_o"),
    )
)

qualified = (
    ranked
    .crossJoin(ranked.agg(F.avg("rows_in_last_agent_hired_on_month_row_count").alias("overall_avg_rows_in_last_agent_hired_on_month_ro")))
    .filter(F.col("rows_in_last_agent_hired_on_month_row_count") > F.col("overall_avg_rows_in_last_agent_hired_on_month_ro"))
    .drop("overall_avg_rows_in_last_agent_hired_on_month_ro")
)

result = (
    qualified
    .select(
        "agent_hired_on_month",
        "max_days_since_opened_at",
        "row_count",
        "rows_in_last_agent_hired_on_month_row_count",
        "total_days_since_opened_at_vs_last_agent_hired_o",
        F.round(F.col("max_days_since_opened_at") / F.nullif(F.col("row_count"), F.lit(0)), 3).alias("max_days_since_opened_at_per_row_count"),
    )
    .orderBy(
        F.col("max_days_since_opened_at").asc(),
        F.col("max_days_since_opened_at_per_row_count").asc(),
    )
)
