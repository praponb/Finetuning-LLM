from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat_by_closed_at_p = Window.partitionBy("agent_id", "csat").orderBy(F.col("closed_at").desc(), "id").rowsBetween(-6, -1)
w_closed_at_hour_priority_by_c = Window.partitionBy("closed_at_hour", "priority").orderBy(F.col("csat").asc_nulls_last())
w_all_by_opened_at_week_ties = Window.orderBy("opened_at_week")
w_all_by_opened_at_week = Window.orderBy("opened_at_week", "id")
w_csat = Window.partitionBy("csat")
w_all_by_opened_at_week_full = Window.orderBy("opened_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_at_week_runnin = Window.orderBy("opened_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_month_by_opened_at = Window.partitionBy(F.date_trunc("month", "opened_at")).orderBy(F.col("opened_at_week_seq").desc(), "id")
w_agent_id = Window.partitionBy("agent_id")
w_opened_at_month_by_opened_at_2 = Window.partitionBy(F.date_trunc("month", "opened_at")).orderBy(F.col("opened_at_week_seq").desc(), "id").rowsBetween(-1, Window.currentRow)
w_opened_at_week_seq_opened_at = Window.partitionBy("opened_at_week_seq", F.year("opened_at"))

base_tickets = (
    df_tickets
    .filter((F.col("agent_id") >= 4) & (F.col("priority") != "P1"))
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
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
        F.date_trunc("week", F.col("opened_at")).alias("opened_at_week"),
    )
)

ranked = (
    base_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
        "opened_at_week",
        F.round(F.avg("closed_at_hour").over(w_agent_id_csat_by_closed_at_p), 2).alias("closed_at_hour_avg_6_prior"),
        F.round(F.percent_rank().over(w_closed_at_hour_priority_by_c), 3).alias("csat_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_opened_at_week_ties), 3).alias("opened_at_week_percent_rank"),
        F.row_number().over(w_all_by_opened_at_week).alias("opened_at_week_seq"),
        F.round(F.cume_dist().over(w_closed_at_hour_priority_by_c), 3).alias("csat_cume_dist"),
        F.min("requester_id").over(w_csat).alias("csat_min_requester_id"),
        F.last("requester_id").over(w_all_by_opened_at_week_full).alias("last_requester_id"),
        F.round(F.cume_dist().over(w_all_by_opened_at_week), 3).alias("opened_at_week_cume_dist"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "requester_id",
        "csat",
        "closed_at_hour_avg_6_prior",
        "opened_at_week_seq",
        "csat_cume_dist",
        "csat_min_requester_id",
        "last_requester_id",
        "opened_at_week_cume_dist",
        F.count(F.lit(1)).over(w_all_by_opened_at_week_runnin).alias("running_count"),
        F.round(F.cume_dist().over(w_opened_at_month_by_opened_at), 3).alias("opened_at_week_seq_cume_dist"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.round(F.avg("opened_at_week_seq").over(w_opened_at_month_by_opened_at_2), 2).alias("opened_at_week_seq_avg_2"),
        F.round(F.col("closed_at_hour_avg_6_prior") / F.nullif(F.sum("closed_at_hour_avg_6_prior").over(w_agent_id), F.lit(0)), 4).alias("closed_at_hour_avg_6_prior_share_of_agent_id"),
        F.count(F.lit(1)).over(w_opened_at_week_seq_opened_at).alias("rows_in_opened_at_week_seq_opened_at_year"),
        F.lead("opened_at_week_percent_rank", 3).over(w_all_by_opened_at_week).alias("next3_opened_at_week_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "requester_id",
        "csat",
        "closed_at_hour_avg_6_prior",
        "opened_at_week_seq",
        "csat_cume_dist",
        "csat_min_requester_id",
        "last_requester_id",
        "opened_at_week_cume_dist",
        "running_count",
        "opened_at_week_seq_cume_dist",
        "rows_in_agent_id",
        "opened_at_week_seq_avg_2",
        "rows_in_opened_at_week_seq_opened_at_year",
        "next3_opened_at_week_percent_rank",
        F.round(F.col("opened_at_week_seq") / F.nullif(F.col("last_requester_id"), F.lit(0)), 3).alias("opened_at_week_seq_per_last_requester_id"),
        (F.floor(F.col("opened_at_week_seq") / 2) * 2).alias("opened_at_week_seq_bucket"),
    )
    .orderBy("closed_at_hour_avg_6_prior", "requester_id")
)
