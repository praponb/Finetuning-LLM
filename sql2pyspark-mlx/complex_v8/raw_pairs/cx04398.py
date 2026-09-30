from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month_by_opened_at = Window.partitionBy(F.date_trunc("month", "opened_at")).orderBy("opened_at", "id")
w_agent_id_by_opened_at_year_f = Window.partitionBy("agent_id").orderBy("opened_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id = Window.partitionBy("agent_id")
w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_all_by_closed_at_ties = Window.orderBy("closed_at")
w_agent_id_by_opened_at_moving = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc_nulls_first(), "id")
w_agent_id_opened_at_to_date = Window.partitionBy("agent_id", F.to_date("opened_at"))
w_opened_at_to_date_by_opened = Window.partitionBy(F.to_date("opened_at")).orderBy("opened_at", "id").rowsBetween(-6, -1)
w_channel_agent_id_by_opened_a = Window.partitionBy("channel", "agent_id").orderBy(F.col("opened_at").desc(), "id")
w_channel_agent_id_by_opened_a_2 = Window.partitionBy("channel", "agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_dense_rank = Window.partitionBy("opened_at_dense_rank")
w_closed_at_year_by_second_ope = Window.partitionBy(F.year("closed_at")).orderBy(F.col("second_opened_at").desc(), "id")
w_closed_at_year_by_second_ope_2 = Window.partitionBy(F.year("closed_at")).orderBy(F.col("second_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .filter(F.col("requester_id") >= 7)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        F.date_trunc("year", F.col("opened_at")).alias("opened_at_year"),
    )
)

sequenced = (
    base
    .select(
        "id",
        "requester_id",
        "closed_at",
        "channel",
        "opened_at_year",
        F.dense_rank().over(w_opened_at_month_by_opened_at).alias("opened_at_dense_rank"),
        F.nth_value("opened_at", 2).over(w_agent_id_by_opened_at_year_f).alias("second_opened_at"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id), 2).alias("requester_id_vs_agent_id_avg"),
        F.round(F.percent_rank().over(w_all_by_closed_at_ties), 3).alias("closed_at_percent_rank"),
        F.sum("requester_id").over(w_agent_id_by_opened_at_moving).alias("requester_id_sum_2"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_agent_id_by_opened_at))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.round(F.avg("requester_id").over(w_agent_id), 2).alias("agent_id_avg_requester_id"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id), F.lit(0)), 4).alias("requester_id_share_of_agent_id"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id_opened_at_to_date), F.lit(0)), 4).alias("requester_id_share_of_agent_id_opened_at_to_date"),
        F.round(F.avg("requester_id").over(w_opened_at_to_date_by_opened), 2).alias("requester_id_avg_6_prior"),
        F.round((F.unix_timestamp("opened_at_year") - F.unix_timestamp(F.lag("opened_at_year").over(w_channel_agent_id_by_opened_a))) / 60, 1).alias("minutes_since_prev_opened_at_year"),
        F.min("requester_id").over(w_channel_agent_id_by_opened_a_2).alias("min_requester_id_so_far"),
        F.max("requester_id").over(w_agent_id).alias("agent_id_max_requester_id"),
    )
)

leaders = sequenced.filter(F.col("opened_at_dense_rank") == 1)

with_trends = (
    leaders
    .select(
        "id",
        "requester_id",
        "channel",
        "opened_at_year",
        "opened_at_dense_rank",
        "second_opened_at",
        "requester_id_vs_agent_id_avg",
        "closed_at_percent_rank",
        "requester_id_sum_2",
        "agent_id_avg_requester_id",
        "requester_id_share_of_agent_id",
        "requester_id_share_of_agent_id_opened_at_to_date",
        "requester_id_avg_6_prior",
        "minutes_since_prev_opened_at_year",
        "min_requester_id_so_far",
        "agent_id_max_requester_id",
        F.size(F.collect_set("channel").over(w_opened_at_dense_rank)).alias("distinct_channel_in_opened_at_dense_rank"),
        F.row_number().over(w_closed_at_year_by_second_ope).alias("second_opened_at_seq"),
        F.last("opened_at_dense_rank", ignorenulls=True).over(w_closed_at_year_by_second_ope_2).alias("last_known_opened_at_dense_rank"),
        F.round(F.col("requester_id_sum_2") - F.avg("requester_id_sum_2").over(w_opened_at_dense_rank), 2).alias("requester_id_sum_2_vs_opened_at_dense_rank_avg"),
        F.sum("requester_id_sum_2").over(w_opened_at_dense_rank).alias("opened_at_dense_rank_sum_requester_id_sum_2"),
        F.dense_rank().over(w_closed_at_year_by_second_ope).alias("second_opened_at_dense_rank"),
        F.round(F.col("requester_id_avg_6_prior") - F.avg("requester_id_avg_6_prior").over(w_opened_at_dense_rank), 2).alias("requester_id_avg_6_prior_vs_opened_at_dense_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "requester_id",
        "channel",
        "opened_at_year",
        "second_opened_at",
        "requester_id_vs_agent_id_avg",
        "closed_at_percent_rank",
        "agent_id_avg_requester_id",
        "requester_id_share_of_agent_id",
        "requester_id_share_of_agent_id_opened_at_to_date",
        "requester_id_avg_6_prior",
        "min_requester_id_so_far",
        "agent_id_max_requester_id",
        "second_opened_at_seq",
        "last_known_opened_at_dense_rank",
        "opened_at_dense_rank_sum_requester_id_sum_2",
        "second_opened_at_dense_rank",
        "requester_id_avg_6_prior_vs_opened_at_dense_rank",
        F.date_trunc("quarter", F.col("second_opened_at")).alias("second_opened_at_quarter"),
        F.month(F.col("second_opened_at")).alias("second_opened_at_month"),
    )
)
