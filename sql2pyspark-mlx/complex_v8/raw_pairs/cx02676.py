from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_opened_at_to_date = Window.partitionBy("priority", F.to_date("opened_at"))
w_all_by_requester_id_ties = Window.orderBy("requester_id")
w_all_by_opened_at_running = Window.orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_to_date_channel_by = Window.partitionBy(F.to_date("opened_at"), "channel").orderBy("opened_at", "id")
w_all_by_requester_id_moving_2 = Window.orderBy("requester_id", "id").rowsBetween(-2, Window.currentRow)
w_all_by_opened_at_prior_6 = Window.orderBy("opened_at", "id").rowsBetween(-6, -1)
w_closed_at_month_channel_by_c = Window.partitionBy(F.month("closed_at"), "channel").orderBy(F.col("csat").desc(), "id")
w_all_by_requester_id_full = Window.orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_month_channel_by_c_2 = Window.partitionBy(F.month("closed_at"), "channel").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_priority_by_opened = Window.partitionBy("agent_id", "priority").orderBy("opened_at", "id")
w_agent_id_closed_at_year = Window.partitionBy("agent_id", F.year("closed_at"))
w_first_csat_channel = Window.partitionBy("first_csat", "channel")
w_first_csat_by_last_known_csa = Window.partitionBy("first_csat").orderBy("last_known_csat", "id")
w_all_by_last_known_csat_prior = Window.orderBy("last_known_csat", "id").rowsBetween(-6, -1)
w_all_by_last_known_csat = Window.orderBy("last_known_csat", "id")

base_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "outage"))
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
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

sequenced = (
    base_tickets
    .select(
        "*",
        F.max("requester_id").over(w_priority_opened_at_to_date).alias("priority_opened_at_to_date_max_requester_id"),
        F.rank().over(w_all_by_requester_id_ties).alias("requester_id_rank"),
        F.last("csat", ignorenulls=True).over(w_all_by_opened_at_running).alias("last_known_csat"),
        F.row_number().over(w_opened_at_to_date_channel_by).alias("rn"),
        F.sum("csat").over(w_all_by_requester_id_moving_2).alias("csat_sum_3"),
        F.sum("requester_id").over(w_all_by_opened_at_prior_6).alias("requester_id_sum_6_prior"),
        F.ntile(3).over(w_closed_at_month_channel_by_c).alias("csat_tercile"),
        F.size(F.collect_set("agent_id").over(w_priority_opened_at_to_date)).alias("distinct_agent_id_in_priority_opened_at_to_date"),
        F.nth_value("requester_id", 2).over(w_all_by_requester_id_full).alias("second_requester_id"),
        F.first("csat").over(w_closed_at_month_channel_by_c_2).alias("first_csat"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_agent_id_priority_by_opened))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.round(F.avg("requester_id").over(w_agent_id_closed_at_year), 2).alias("agent_id_closed_at_year_avg_requester_id"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        "last_known_csat",
        "rn",
        "requester_id_sum_6_prior",
        "distinct_agent_id_in_priority_opened_at_to_date",
        "minutes_since_prev_opened_at",
        "agent_id_closed_at_year_avg_requester_id",
        F.count(F.lit(1)).over(w_first_csat_channel).alias("rows_in_first_csat_channel"),
        F.max("csat").over(w_first_csat_channel).alias("first_csat_channel_max_csat"),
        F.ntile(3).over(w_first_csat_by_last_known_csa).alias("last_known_csat_tercile"),
        F.round(F.avg("requester_id_sum_6_prior").over(w_all_by_last_known_csat_prior), 2).alias("requester_id_sum_6_prior_avg_6_prior"),
        F.lag("minutes_since_prev_opened_at").over(w_first_csat_by_last_known_csa).alias("prev_minutes_since_prev_opened_at"),
        F.ntile(3).over(w_all_by_last_known_csat).alias("last_known_csat_tercile_2"),
    )
)

result = (
    with_trends
    .filter(F.col("agent_id_closed_at_year_avg_requester_id") > 11.8)
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "tags",
        "last_known_csat",
        "rn",
        "requester_id_sum_6_prior",
        "distinct_agent_id_in_priority_opened_at_to_date",
        "minutes_since_prev_opened_at",
        "agent_id_closed_at_year_avg_requester_id",
        "requester_id_sum_6_prior_avg_6_prior",
        "prev_minutes_since_prev_opened_at",
        "last_known_csat_tercile_2",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
    )
    .orderBy(F.col("last_known_csat").asc_nulls_last(), "id")
    .limit(20)
)
