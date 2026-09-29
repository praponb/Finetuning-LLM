from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_at_hour_runnin = Window.orderBy("opened_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_channel = Window.partitionBy("priority", "channel")
w_channel_opened_at_hour_by_op = Window.partitionBy("channel", "opened_at_hour").orderBy(F.col("opened_at_hour").desc_nulls_first(), "id")
w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_channel_opened_at_hour_by_op_2 = Window.partitionBy("channel", "opened_at_hour").orderBy(F.col("opened_at_hour").desc_nulls_first())
w_opened_at_hour_priority_by_o = Window.partitionBy("opened_at_hour", "priority").orderBy("opened_at_hour", "id").rowsBetween(-1, -1)
w_channel_opened_at_hour_by_op_3 = Window.partitionBy("channel", "opened_at_hour").orderBy(F.col("opened_at_hour").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_opened_at_hour_priority_by_o_2 = Window.partitionBy("opened_at_hour", "priority").orderBy("opened_at_hour")
w_channel_opened_at_hour_by_op_4 = Window.partitionBy("channel", "opened_at_hour").orderBy(F.col("opened_at_hour").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_opened_at_hour_by_o = Window.partitionBy("priority", "opened_at_hour").orderBy("opened_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev2_opened_at_hour_opened = Window.partitionBy("prev2_opened_at_hour", "opened_at_hour").orderBy("priority_channel_min_opened_at_hour", "id").rowsBetween(-1, -1)
w_last_opened_at_hour = Window.partitionBy("last_opened_at_hour")

recent_tickets = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-03-09 04:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "tags",
        F.hour(F.col("opened_at")).alias("opened_at_hour"),
        F.concat_ws(": ", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

metrics = (
    recent_tickets
    .select(
        "*",
        F.first("opened_at_hour", ignorenulls=True).over(w_all_by_opened_at_hour_runnin).alias("first_known_opened_at_hour"),
        F.min("opened_at_hour").over(w_priority_channel).alias("priority_channel_min_opened_at_hour"),
        (F.col("opened_at_hour") - F.lag("opened_at_hour").over(w_channel_opened_at_hour_by_op)).alias("opened_at_hour_change"),
        F.round(F.col("opened_at_hour") - F.avg("opened_at_hour").over(w_agent_id_channel), 2).alias("opened_at_hour_vs_agent_id_channel_avg"),
        F.round(F.percent_rank().over(w_channel_opened_at_hour_by_op_2), 3).alias("opened_at_hour_percent_rank"),
        F.sum("opened_at_hour").over(w_opened_at_hour_priority_by_o).alias("opened_at_hour_sum_1_prior"),
        F.count(F.lit(1)).over(w_agent_id_channel).alias("rows_in_agent_id_channel"),
        F.size(F.collect_set("priority").over(w_agent_id_channel)).alias("distinct_priority_in_agent_id_channel"),
        F.sum("opened_at_hour").over(w_channel_opened_at_hour_by_op_3).alias("opened_at_hour_sum_2"),
        F.dense_rank().over(w_opened_at_hour_priority_by_o_2).alias("opened_at_hour_dense_rank"),
        F.lag("opened_at_hour", 2, 0).over(w_channel_opened_at_hour_by_op).alias("prev2_opened_at_hour"),
        F.nth_value("opened_at_hour", 2).over(w_channel_opened_at_hour_by_op_4).alias("second_opened_at_hour"),
        F.last("opened_at_hour").over(w_priority_opened_at_hour_by_o).alias("last_opened_at_hour"),
    )
)

first_rows = metrics.filter(F.col("opened_at_hour_dense_rank") == 1)

windowed = (
    first_rows
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "opened_at_hour",
        "channel_priority_label",
        "first_known_opened_at_hour",
        "priority_channel_min_opened_at_hour",
        "opened_at_hour_change",
        "opened_at_hour_percent_rank",
        "opened_at_hour_sum_1_prior",
        "rows_in_agent_id_channel",
        "distinct_priority_in_agent_id_channel",
        "prev2_opened_at_hour",
        "second_opened_at_hour",
        "last_opened_at_hour",
        F.max("last_opened_at_hour").over(w_prev2_opened_at_hour_opened).alias("last_opened_at_hour_max_1_prior"),
        F.round(F.col("rows_in_agent_id_channel") - F.avg("rows_in_agent_id_channel").over(w_last_opened_at_hour), 2).alias("rows_in_agent_id_channel_vs_last_opened_at_hour_"),
        F.round(F.col("opened_at_hour_sum_1_prior") / F.nullif(F.sum("opened_at_hour_sum_1_prior").over(w_last_opened_at_hour), F.lit(0)), 4).alias("opened_at_hour_sum_1_prior_share_of_last_opened_"),
        F.count(F.lit(1)).over(w_last_opened_at_hour).alias("rows_in_last_opened_at_hour"),
        F.min("opened_at_hour_sum_1_prior").over(w_last_opened_at_hour).alias("last_opened_at_hour_min_opened_at_hour_sum_1_pri"),
    )
)

summary = (
    windowed
    .filter(F.col("channel_priority_label").isNotNull())
    .groupBy("last_opened_at_hour")
    .agg(
        F.round(F.sum("first_known_opened_at_hour") / F.nullif(F.sum("priority_channel_min_opened_at_hour"), F.lit(0)), 3).alias("first_known_opened_at_hour_to_priority_channel_m"),
        F.sum(F.when(F.col("channel") == "chat", F.col("opened_at_hour_sum_1_prior")).otherwise(0)).alias("opened_at_hour_sum_1_prior_channel_chat"),
        F.round(F.sum("prev2_opened_at_hour") / F.nullif(F.sum("opened_at_hour_percent_rank"), F.lit(0)), 3).alias("prev2_opened_at_hour_to_opened_at_hour_percent_r"),
        F.percentile_approx("rows_in_last_opened_at_hour", 0.5).alias("median_rows_in_last_opened_at_hour"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
    )
)

result = (
    summary
    .select(
        "last_opened_at_hour",
        "first_known_opened_at_hour_to_priority_channel_m",
        "opened_at_hour_sum_1_prior_channel_chat",
        "prev2_opened_at_hour_to_opened_at_hour_percent_r",
    )
)
