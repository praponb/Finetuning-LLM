from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_csat = Window.partitionBy("csat")
w_agent_id_by_closed_at_full = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_requester_id = Window.orderBy("requester_id", "id")
w_all_by_prev_opened_at_week_p = Window.orderBy(F.col("prev_opened_at_week").desc(), "id").rowsBetween(-2, -1)
w_next_closed_at_month_by_csat = Window.partitionBy(F.date_trunc("month", "next_closed_at")).orderBy("csat", "id")
w_closed_at_date_month = Window.partitionBy(F.month("closed_at_date"))
w_csat_opened_at_dayofweek_by = Window.partitionBy("csat", "opened_at_dayofweek").orderBy(F.col("closed_at").desc())
w_csat_opened_at_dayofweek_by_2 = Window.partitionBy("csat", "opened_at_dayofweek").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_tickets
    .filter(F.col("requester_id") <= 20)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        F.date_trunc("week", F.col("opened_at")).alias("opened_at_week"),
    )
)

with_trends = (
    base
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "csat",
        "opened_at_week",
        F.lag("csat").over(w_agent_id_by_closed_at).alias("prev_csat"),
        F.lead("closed_at").over(w_agent_id_by_closed_at).alias("next_closed_at"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat), F.lit(0)), 4).alias("csat_share_of_csat"),
        F.lag("opened_at_week").over(w_agent_id_by_closed_at).alias("prev_opened_at_week"),
        F.first("requester_id").over(w_agent_id_by_closed_at_full).alias("first_requester_id"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "csat",
        "opened_at_week",
        "next_closed_at",
        "csat_share_of_csat",
        "first_requester_id",
        F.lead("opened_at").over(w_all_by_requester_id).alias("next_opened_at"),
        F.sum("requester_id").over(w_all_by_prev_opened_at_week_p).alias("requester_id_sum_2_prior"),
    )
)

with_activity = (
    metrics
    .crossJoin(metrics.agg(F.avg("csat_share_of_csat").alias("overall_avg_csat_share_of_csat")))
    .filter(F.col("csat_share_of_csat") > F.col("overall_avg_csat_share_of_csat"))
    .drop("overall_avg_csat_share_of_csat")
)

derived = (
    with_activity
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "csat",
        "next_closed_at",
        "csat_share_of_csat",
        "first_requester_id",
        "requester_id_sum_2_prior",
        F.round(F.col("csat") / F.nullif(F.col("first_requester_id"), F.lit(0)), 3).alias("csat_per_first_requester_id"),
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)

ranked = (
    derived
    .select(
        "*",
        F.round(F.cume_dist().over(w_next_closed_at_month_by_csat), 3).alias("csat_cume_dist"),
        F.lead("requester_id").over(w_next_closed_at_month_by_csat).alias("next_requester_id"),
        F.round(F.col("first_requester_id") / F.nullif(F.sum("first_requester_id").over(w_closed_at_date_month), F.lit(0)), 4).alias("first_requester_id_share_of_closed_at_date_month"),
        F.rank().over(w_csat_opened_at_dayofweek_by).alias("closed_at_rank"),
        F.round(F.col("first_requester_id") - F.avg("first_requester_id").over(w_closed_at_date_month), 2).alias("first_requester_id_vs_closed_at_date_month_avg"),
        F.nth_value("csat_per_first_requester_id", 2).over(w_csat_opened_at_dayofweek_by_2).alias("second_csat_per_first_requester_id"),
    )
)

enriched = (
    ranked
    .filter(F.col("first_requester_id_share_of_closed_at_date_month") > 0.1)
    .select(
        "id",
        "requester_id",
        "closed_at",
        "csat",
        "next_closed_at",
        "first_requester_id",
        "closed_at_date",
        "csat_cume_dist",
        "next_requester_id",
        "first_requester_id_vs_closed_at_date_month_avg",
        F.quarter(F.col("closed_at_date")).alias("closed_at_date_quarter"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at_2"),
    )
)

result = (
    enriched
    .select("csat", "closed_at_date_quarter", "closed_at_date")
    .distinct()
    .orderBy(F.col("closed_at_date_quarter").desc_nulls_first())
)
