from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_csat_by_csat_prior_1 = Window.partitionBy("channel", "csat").orderBy(F.col("csat").desc(), "id").rowsBetween(-1, -1)
w_closed_at_month_csat_by_clos = Window.partitionBy(F.date_trunc("month", "closed_at"), "csat").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_closed_at_month_by_opened_at = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("opened_at").desc(), "id")
w_channel_csat_by_csat = Window.partitionBy("channel", "csat").orderBy(F.col("csat").desc(), "id")
w_closed_at_month_by_opened_at_2 = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("opened_at").desc())
w_closed_at_month_by_opened_at_3 = Window.partitionBy(F.date_trunc("month", "closed_at")).orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_csat_by_csat_running = Window.partitionBy("channel", "csat").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_month_csat_by_clos_2 = Window.partitionBy(F.date_trunc("month", "closed_at"), "csat").orderBy(F.col("closed_at").desc())
w_opened_at_rank_by_agent_id_c = Window.partitionBy("opened_at_rank").orderBy("agent_id_channel_avg_requester_id", "id").rowsBetween(-3, Window.currentRow)
w_opened_at_rank_by_agent_id_c_2 = Window.partitionBy("opened_at_rank").orderBy("agent_id_channel_avg_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_rank_by_agent_id_c_3 = Window.partitionBy("opened_at_rank").orderBy("agent_id_channel_avg_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_rank_distinct_prio = Window.partitionBy("opened_at_rank", "distinct_priority_in_agent_id_channel")
w_channel_agent_id_by_closed_a = Window.partitionBy("channel", "agent_id").orderBy("closed_at_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
    df_tickets
    .filter(F.col("requester_id") <= 23)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

ranked = (
    base_tickets
    .select(
        "*",
        F.round(F.avg("csat").over(w_channel_csat_by_csat_prior_1), 2).alias("csat_avg_1_prior"),
        F.first("csat").over(w_closed_at_month_csat_by_clos).alias("first_csat"),
        F.min("requester_id").over(w_agent_id_channel).alias("agent_id_channel_min_requester_id"),
        F.lead("closed_at").over(w_closed_at_month_by_opened_at).alias("next_closed_at"),
        F.round(F.avg("requester_id").over(w_agent_id_channel), 2).alias("agent_id_channel_avg_requester_id"),
        F.lag("closed_at").over(w_channel_csat_by_csat).alias("prev_closed_at"),
        F.rank().over(w_closed_at_month_by_opened_at_2).alias("opened_at_rank"),
        F.min("csat").over(w_agent_id_channel).alias("agent_id_channel_min_csat"),
        F.first("opened_at").over(w_closed_at_month_by_opened_at_3).alias("first_opened_at"),
        F.last("opened_at", ignorenulls=True).over(w_channel_csat_by_csat_running).alias("last_known_opened_at"),
        F.round(F.percent_rank().over(w_closed_at_month_csat_by_clos_2), 3).alias("closed_at_percent_rank"),
        F.size(F.collect_set("priority").over(w_agent_id_channel)).alias("distinct_priority_in_agent_id_channel"),
    )
)

top_rows = ranked.filter(F.col("opened_at_rank") <= 5)

with_windows = (
    top_rows
    .select(
        "*",
        F.max("agent_id_channel_min_csat").over(w_opened_at_rank_by_agent_id_c).alias("agent_id_channel_min_csat_max_4"),
        F.first("agent_id_channel_min_csat", ignorenulls=True).over(w_opened_at_rank_by_agent_id_c_2).alias("first_known_agent_id_channel_min_csat"),
        F.nth_value("csat", 2).over(w_opened_at_rank_by_agent_id_c_3).alias("second_csat"),
        F.max("requester_id").over(w_opened_at_rank_distinct_prio).alias("opened_at_rank_distinct_priority_in_agent_id_cha"),
        F.sum("agent_id_channel_min_csat").over(w_channel_agent_id_by_closed_a).alias("running_agent_id_channel_min_csat"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "requester_id",
        "closed_at",
        "channel",
        "csat",
        "csat_avg_1_prior",
        "first_csat",
        "agent_id_channel_min_requester_id",
        "next_closed_at",
        "agent_id_channel_avg_requester_id",
        "prev_closed_at",
        "opened_at_rank",
        "agent_id_channel_min_csat",
        "first_opened_at",
        "last_known_opened_at",
        "closed_at_percent_rank",
        "distinct_priority_in_agent_id_channel",
        "agent_id_channel_min_csat_max_4",
        "first_known_agent_id_channel_min_csat",
        "running_agent_id_channel_min_csat",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
    .orderBy(F.col("opened_at_rank").desc())
)
