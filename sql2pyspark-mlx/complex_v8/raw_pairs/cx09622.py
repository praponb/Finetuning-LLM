from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_priority_len = Window.partitionBy("agent_id").orderBy("priority_len", "id")
w_channel = Window.partitionBy("channel")
w_csat_agent_id_by_csat = Window.partitionBy("csat", "agent_id").orderBy(F.col("csat").desc_nulls_first(), "id")
w_channel_by_priority_len_runn = Window.partitionBy("channel").orderBy(F.col("priority_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_agent_id_by_csat_prior = Window.partitionBy("csat", "agent_id").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_csat_agent_id_by_csat_full = Window.partitionBy("csat", "agent_id").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_by_priority_len = Window.partitionBy("channel").orderBy(F.col("priority_len").desc(), "id")
w_channel_by_priority_len_ties = Window.partitionBy("channel").orderBy(F.col("priority_len").desc())
w_agent_id_by_priority_len_run = Window.partitionBy("agent_id").orderBy("priority_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_len_rank = Window.partitionBy("priority_len_rank")
w_distinct_channels_by_priorit = Window.partitionBy("distinct_channels").orderBy(F.col("priority_len_dense_rank_channel_not_in_email_pho").desc_nulls_first(), "priority_len_rank", "channel").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_channels_by_priorit_2 = Window.partitionBy("distinct_channels").orderBy(F.col("priority_len_dense_rank_channel_not_in_email_pho").desc_nulls_first(), "priority_len_rank", "channel")

valid_tickets = (
    df_tickets
    .filter(F.col("channel").isin("phone", "chat"))
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
    )
)

metrics = (
    valid_tickets
    .select(
        "id",
        "channel",
        "priority_len",
        "requester_id_plus_csat",
        (F.col("requester_id_plus_csat") - F.lag("requester_id_plus_csat").over(w_agent_id_by_priority_len)).alias("requester_id_plus_csat_change"),
        F.round(F.col("priority_len") / F.nullif(F.sum("priority_len").over(w_channel), F.lit(0)), 4).alias("priority_len_share_of_channel"),
        (F.col("requester_id_plus_csat") - F.lag("requester_id_plus_csat").over(w_csat_agent_id_by_csat)).alias("requester_id_plus_csat_change_2"),
        F.count(F.lit(1)).over(w_channel_by_priority_len_runn).alias("running_count"),
        F.round(F.avg("requester_id_plus_csat").over(w_csat_agent_id_by_csat_prior), 2).alias("requester_id_plus_csat_avg_2_prior"),
        F.nth_value("priority_len", 2).over(w_csat_agent_id_by_csat_full).alias("second_priority_len"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
        F.max("priority_len").over(w_channel).alias("channel_max_priority_len"),
        F.rank().over(w_channel_by_priority_len).alias("priority_len_rank"),
        F.dense_rank().over(w_channel_by_priority_len_ties).alias("priority_len_dense_rank"),
        F.first("requester_id_plus_csat", ignorenulls=True).over(w_agent_id_by_priority_len_run).alias("first_known_requester_id_plus_csat"),
        F.round(F.col("priority_len") - F.avg("priority_len").over(w_channel), 2).alias("priority_len_vs_channel_avg"),
    )
)

per_group = (
    metrics
    .filter(F.col("requester_id_plus_csat_change") > -15)
    .groupBy("priority_len_rank", "channel")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("priority_len", 0.5).alias("median_priority_len"),
        F.round(F.avg("first_known_requester_id_plus_csat"), 2).alias("avg_first_known_requester_id_plus_csat"),
        F.countDistinct("channel").alias("distinct_channels"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(~F.col("channel").isin("email", "phone"), F.col("priority_len_dense_rank"))).alias("priority_len_dense_rank_channel_not_in_email_pho"),
    )
)

with_trends = (
    per_group
    .select(
        "priority_len_rank",
        "channel",
        "cnt",
        "median_priority_len",
        "distinct_ids",
        "priority_len_dense_rank_channel_not_in_email_pho",
        F.round(F.col("distinct_channels") - F.avg("distinct_channels").over(w_priority_len_rank), 2).alias("distinct_channels_vs_priority_len_rank_avg"),
        F.first("distinct_ids").over(w_distinct_channels_by_priorit).alias("first_distinct_ids"),
        F.ntile(10).over(w_distinct_channels_by_priorit_2).alias("priority_len_dense_rank_channel_not_in_email_pho_2"),
    )
)

result = (
    with_trends
    .select(
        "priority_len_rank",
        "median_priority_len",
        "distinct_ids",
        "priority_len_dense_rank_channel_not_in_email_pho",
        "distinct_channels_vs_priority_len_rank_avg",
        "first_distinct_ids",
        "priority_len_dense_rank_channel_not_in_email_pho_2",
        F.when(F.col("channel").isin("chat", "phone"), "group_a").when(F.col("channel") == "web", "group_b").otherwise("other").alias("channel_group"),
    )
    .orderBy(F.col("median_priority_len").desc())
)
