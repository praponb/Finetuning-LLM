from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_priority_by_requeste = Window.partitionBy("channel", "priority").orderBy(F.col("requester_id_per_csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_requester_id_per = Window.partitionBy("priority").orderBy(F.col("requester_id_per_csat").desc_nulls_first(), "id")
w_priority_by_opened_at = Window.partitionBy("priority").orderBy("opened_at", "id")
w_channel = Window.partitionBy("channel")
w_channel_priority_by_requeste_2 = Window.partitionBy("channel", "priority").orderBy(F.col("requester_id_per_csat").desc_nulls_first(), "id")
w_priority_by_opened_at_full = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_by_opened_at_moving = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(-6, Window.currentRow)
w_priority_by_opened_at_runnin = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_requester_id_per_2 = Window.partitionBy("priority").orderBy(F.col("requester_id_per_csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_priority_in_channel = Window.partitionBy("distinct_priority_in_channel", "opened_at_seq")
w_distinct_priority_in_channel_2 = Window.partitionBy("distinct_priority_in_channel", "channel").orderBy("requester_id_per_csat_share_of_channel", "id")
w_distinct_priority_in_channel_3 = Window.partitionBy("distinct_priority_in_channel", "channel").orderBy("requester_id_per_csat_share_of_channel")
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_at = Window.orderBy(F.col("opened_at").desc(), "id")

cleaned = (
    df_tickets
    .filter(F.col("csat").between(2, 5))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.last("requester_id_per_csat", ignorenulls=True).over(w_channel_priority_by_requeste).alias("last_known_requester_id_per_csat"),
        F.lead("requester_id_per_csat").over(w_priority_by_requester_id_per).alias("next_requester_id_per_csat"),
        F.row_number().over(w_priority_by_opened_at).alias("opened_at_seq"),
        F.round(F.col("requester_id_per_csat") / F.nullif(F.sum("requester_id_per_csat").over(w_channel), F.lit(0)), 4).alias("requester_id_per_csat_share_of_channel"),
        (F.col("requester_id_per_csat") - F.lag("requester_id_per_csat").over(w_channel_priority_by_requeste_2)).alias("requester_id_per_csat_change"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.nth_value("requester_id_per_csat", 2).over(w_priority_by_opened_at_full).alias("second_requester_id_per_csat"),
        F.dense_rank().over(w_priority_by_opened_at).alias("opened_at_dense_rank"),
        F.round(F.percent_rank().over(w_priority_by_requester_id_per), 3).alias("requester_id_per_csat_percent_rank"),
        F.round(F.col("requester_id_per_csat") - F.avg("requester_id_per_csat").over(w_channel), 2).alias("requester_id_per_csat_vs_channel_avg"),
        F.max("requester_id_per_csat").over(w_priority_by_opened_at_moving).alias("requester_id_per_csat_max_7"),
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.sum("requester_id_per_csat").over(w_priority_by_opened_at_runnin).alias("running_requester_id_per_csat"),
        F.last("opened_at", ignorenulls=True).over(w_priority_by_requester_id_per_2).alias("last_known_opened_at"),
        F.size(F.collect_set("priority").over(w_channel)).alias("distinct_priority_in_channel"),
    )
)

first_rows = ranked.filter(F.col("opened_at_dense_rank") == 1)

sequenced = (
    first_rows
    .select(
        "id",
        "agent_id",
        "channel",
        "requester_id_per_csat",
        "last_known_requester_id_per_csat",
        "next_requester_id_per_csat",
        "opened_at_seq",
        "requester_id_per_csat_share_of_channel",
        "requester_id_per_csat_change",
        "rows_in_channel",
        "second_requester_id_per_csat",
        "requester_id_per_csat_percent_rank",
        "requester_id_per_csat_vs_channel_avg",
        "running_requester_id_per_csat",
        "last_known_opened_at",
        "distinct_priority_in_channel",
        F.round(F.col("rows_in_channel") - F.avg("rows_in_channel").over(w_distinct_priority_in_channel), 2).alias("rows_in_channel_vs_distinct_priority_in_channel_"),
        F.row_number().over(w_distinct_priority_in_channel_2).alias("seq_no"),
        F.rank().over(w_distinct_priority_in_channel_3).alias("requester_id_per_csat_share_of_channel_rank"),
        F.first("opened_at_seq").over(w_all_by_opened_at_full).alias("first_opened_at_seq"),
        (F.col("next_requester_id_per_csat") - F.lag("next_requester_id_per_csat").over(w_all_by_opened_at)).alias("next_requester_id_per_csat_change"),
        F.sum("requester_id_per_csat_max_7").over(w_distinct_priority_in_channel).alias("distinct_priority_in_channel_opened_at_seq_sum_r"),
        F.sum("second_requester_id_per_csat").over(w_distinct_priority_in_channel).alias("distinct_priority_in_channel_opened_at_seq_sum_s"),
    )
)

grouped = (
    sequenced
    .groupBy(
        "agent_id",
        F.to_date("last_known_opened_at").alias("last_known_opened_at_day"),
    )
    .agg(
        F.max("last_known_opened_at").alias("last_last_known_opened_at"),
        F.min("running_requester_id_per_csat").alias("min_running_requester_id_per_csat"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.sum(F.when(~F.col("channel").isin("email", "phone"), F.col("requester_id_per_csat_share_of_channel_rank")).otherwise(0)).alias("requester_id_per_csat_share_of_channel_rank_chan"),
        F.count("*").alias("cnt"),
        F.min("first_opened_at_seq").alias("min_first_opened_at_seq"),
    )
)

result = (
    grouped
    .select(
        "agent_id",
        "last_known_opened_at_day",
        "last_last_known_opened_at",
        "min_running_requester_id_per_csat",
        "distinct_channels",
        "requester_id_per_csat_share_of_channel_rank_chan",
        "cnt",
        "min_first_opened_at_seq",
        F.when(F.col("min_first_opened_at_seq") >= 9, "large").when(F.col("min_first_opened_at_seq") >= 5, "medium").otherwise("small").alias("min_first_opened_at_seq_tier"),
    )
)
