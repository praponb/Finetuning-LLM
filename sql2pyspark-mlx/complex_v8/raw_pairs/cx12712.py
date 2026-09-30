from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_opened_at = Window.partitionBy("channel").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_channel = Window.partitionBy("channel")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_csat = Window.partitionBy("csat")
w_csat_priority_by_csat = Window.partitionBy("csat", "priority").orderBy(F.col("csat").desc(), "id")
w_csat_priority_by_csat_ties = Window.partitionBy("csat", "priority").orderBy(F.col("csat").desc())
w_channel_by_opened_at_moving = Window.partitionBy("channel").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_opened_at = Window.orderBy("opened_at", "id")
w_agent_id_distinct_channel_in = Window.partitionBy("agent_id", "distinct_channel_in_csat").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_channel_group_opened_at_seq = Window.partitionBy("channel_group", "opened_at_seq")

cleaned_tickets = (
    df_tickets
    .filter(F.col("requester_id") < 23)
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
    )
)

with_windows = (
    cleaned_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        F.row_number().over(w_channel_by_opened_at).alias("opened_at_seq"),
        F.round(F.col("csat") - F.avg("csat").over(w_channel), 2).alias("csat_vs_channel_avg"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.size(F.collect_set("priority").over(w_channel)).alias("distinct_priority_in_channel"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_requester_id))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.round(F.avg("csat").over(w_channel), 2).alias("channel_avg_csat"),
        F.min("csat").over(w_channel).alias("channel_min_csat"),
        F.size(F.collect_set("channel").over(w_csat)).alias("distinct_channel_in_csat"),
        (F.col("csat") - F.lag("csat").over(w_csat_priority_by_csat)).alias("csat_change"),
        F.rank().over(w_csat_priority_by_csat_ties).alias("csat_rank"),
        F.sum("requester_id").over(w_channel_by_opened_at_moving).alias("requester_id_sum_3"),
        F.round(F.cume_dist().over(w_csat_priority_by_csat_ties), 3).alias("csat_cume_dist"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
        F.when(F.col("channel").isin("email", "web"), "group_a").when(F.col("channel") == "phone", "group_b").otherwise("other").alias("channel_group"),
    )
)

with_windows_2 = (
    enriched
    .select(
        "*",
        F.rank().over(w_all_by_opened_at).alias("opened_at_rank"),
        F.lead("csat_vs_channel_avg", 3).over(w_agent_id_distinct_channel_in).alias("next3_csat_vs_channel_avg"),
        F.max("rows_in_channel").over(w_channel_group_opened_at_seq).alias("channel_group_opened_at_seq_max_rows_in_channel"),
        F.size(F.collect_set("agent_id").over(w_channel_group_opened_at_seq)).alias("distinct_agent_id_in_channel_group_opened_at_seq"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "opened_at_seq",
        "csat_vs_channel_avg",
        "rows_in_channel",
        "minutes_since_prev_closed_at",
        "channel_avg_csat",
        "channel_min_csat",
        "csat_change",
        "requester_id_sum_3",
        "csat_cume_dist",
        "channel_group",
        "opened_at_rank",
        "channel_group_opened_at_seq_max_rows_in_channel",
        "distinct_agent_id_in_channel_group_opened_at_seq",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
    .orderBy(F.col("csat_change").desc())
)
