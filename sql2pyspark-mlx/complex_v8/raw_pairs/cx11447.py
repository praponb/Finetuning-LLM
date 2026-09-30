from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy(F.col("csat").desc(), "id")
w_channel = Window.partitionBy("channel")
w_csat_channel_by_csat_minus_r = Window.partitionBy("csat", "channel").orderBy(F.col("csat_minus_requester_id").desc(), "id").rowsBetween(-2, -1)
w_agent_id_by_csat_prior_1 = Window.partitionBy("agent_id").orderBy(F.col("csat").desc(), "id").rowsBetween(-1, -1)
w_agent_id_csat_by_requester_i = Window.partitionBy("agent_id", "csat").orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_csat_by_requester_i_2 = Window.partitionBy("agent_id", "csat").orderBy(F.col("requester_id").desc())
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")
w_csat_opened_at_to_date = Window.partitionBy("csat", F.to_date("opened_at"))
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc())
w_channel_last_known_closed_at = Window.partitionBy("channel", F.month("last_known_closed_at")).orderBy(F.col("last_known_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_dense_rank_by_csat_minu = Window.partitionBy("csat_dense_rank").orderBy(F.col("csat_minus_requester_id_sum_2_prior").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_csat_dense_rank_by_csat_minu_2 = Window.partitionBy("csat_dense_rank").orderBy(F.col("csat_minus_requester_id_sum_2_prior").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_channel_last_known_closed_at_2 = Window.partitionBy("channel", F.month("last_known_closed_at")).orderBy(F.col("last_known_opened_at").desc(), "id")

base_tickets = (
    df_tickets
    .filter((F.col("agent_id") >= 3) | (F.col("channel").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        (F.col("csat") - F.col("requester_id")).alias("csat_minus_requester_id"),
    )
)

with_windows = (
    base_tickets
    .select(
        "id",
        "requester_id",
        "opened_at",
        "channel",
        "tags",
        "channel_slug",
        "csat_minus_requester_id",
        F.last("closed_at", ignorenulls=True).over(w_all_by_opened_at_running).alias("last_known_closed_at"),
        F.dense_rank().over(w_agent_id_by_csat).alias("csat_dense_rank"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.last("opened_at", ignorenulls=True).over(w_all_by_opened_at_running).alias("last_known_opened_at"),
        F.sum("csat_minus_requester_id").over(w_csat_channel_by_csat_minus_r).alias("csat_minus_requester_id_sum_2_prior"),
        F.max("requester_id").over(w_agent_id_by_csat_prior_1).alias("requester_id_max_1_prior"),
        F.min("requester_id").over(w_agent_id_csat_by_requester_i).alias("min_requester_id_so_far"),
        F.round(F.cume_dist().over(w_agent_id_csat_by_requester_i_2), 3).alias("requester_id_cume_dist"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_all_by_opened_at))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.max("requester_id").over(w_csat_opened_at_to_date).alias("csat_opened_at_to_date_max_requester_id"),
        F.round(F.cume_dist().over(w_all_by_opened_at_ties), 3).alias("opened_at_cume_dist"),
        F.dense_rank().over(w_agent_id_csat_by_requester_i_2).alias("requester_id_dense_rank"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "requester_id",
        "opened_at",
        "tags",
        "channel_slug",
        "csat_minus_requester_id",
        "last_known_closed_at",
        "last_known_opened_at",
        "requester_id_max_1_prior",
        "min_requester_id_so_far",
        "requester_id_cume_dist",
        "csat_opened_at_to_date_max_requester_id",
        "opened_at_cume_dist",
        F.last("last_known_opened_at").over(w_channel_last_known_closed_at).alias("last_last_known_opened_at"),
        F.sum("min_requester_id_so_far").over(w_csat_dense_rank_by_csat_minu).alias("min_requester_id_so_far_sum_7"),
        F.round(F.avg("csat_dense_rank").over(w_csat_dense_rank_by_csat_minu_2), 2).alias("csat_dense_rank_avg_3"),
        F.round(F.col("csat_minus_requester_id") / F.nullif(F.sum("csat_minus_requester_id").over(w_channel), F.lit(0)), 4).alias("csat_minus_requester_id_share_of_channel"),
        F.ntile(5).over(w_channel_last_known_closed_at_2).alias("last_known_opened_at_quintile"),
    )
)

result = (
    windowed
    .filter(F.col("opened_at_cume_dist") > 0.1)
    .select(
        "id",
        "requester_id",
        "opened_at",
        "tags",
        "channel_slug",
        "csat_minus_requester_id",
        "last_known_closed_at",
        "last_known_opened_at",
        "requester_id_max_1_prior",
        "min_requester_id_so_far",
        "requester_id_cume_dist",
        "csat_opened_at_to_date_max_requester_id",
        "opened_at_cume_dist",
        "min_requester_id_so_far_sum_7",
        "csat_dense_rank_avg_3",
        "csat_minus_requester_id_share_of_channel",
        "last_known_opened_at_quintile",
        (F.col("requester_id_max_1_prior") - F.col("csat_minus_requester_id")).alias("requester_id_max_1_prior_minus_csat_minus_reques"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
    .orderBy("last_known_opened_at", "id")
    .limit(10)
)
