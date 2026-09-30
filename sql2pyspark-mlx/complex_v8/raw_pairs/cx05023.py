from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_closed_at_year_by_r = Window.partitionBy("agent_id", F.year("closed_at")).orderBy(F.col("requester_id").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_closed_at_year_by_r_2 = Window.partitionBy("agent_id", F.year("closed_at")).orderBy(F.col("requester_id").desc(), "id").rowsBetween(-3, -1)
w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_channel_csat_by_requester_id = Window.partitionBy("channel", "csat").orderBy("requester_id", "id")
w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id")
w_channel_csat_by_requester_id_2 = Window.partitionBy("channel", "csat").orderBy("requester_id", "id").rowsBetween(-2, -1)
w_agent_id_channel = Window.partitionBy("agent_id", "channel")
w_channel_csat_by_requester_id_3 = Window.partitionBy("channel", "csat").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_seq_no_last_closed_at_year = Window.partitionBy("seq_no", F.year("last_closed_at"))
w_all_by_requester_id_ties = Window.orderBy("requester_id")
w_seq_no_agent_id_by_requester = Window.partitionBy("seq_no", "agent_id").orderBy("requester_id_share_of_agent_id_priority", "id")

cleaned_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("csat") < 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

metrics = (
    cleaned_tickets
    .select(
        "*",
        F.sum("requester_id").over(w_agent_id_closed_at_year_by_r).alias("requester_id_sum_2"),
        F.last("closed_at").over(w_all_by_closed_at_full).alias("last_closed_at"),
        F.round(F.avg("csat").over(w_agent_id_closed_at_year_by_r_2), 2).alias("csat_avg_3_prior"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_id_priority), F.lit(0)), 4).alias("requester_id_share_of_agent_id_priority"),
        F.round(F.cume_dist().over(w_channel_csat_by_requester_id), 3).alias("requester_id_cume_dist"),
        F.round(F.avg("csat").over(w_agent_id_priority), 2).alias("agent_id_priority_avg_csat"),
        F.row_number().over(w_agent_id_by_closed_at).alias("seq_no"),
        F.min("requester_id").over(w_agent_id_priority).alias("agent_id_priority_min_requester_id"),
        F.sum("csat").over(w_channel_csat_by_requester_id_2).alias("csat_sum_2_prior"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_agent_id_channel), 2).alias("requester_id_vs_agent_id_channel_avg"),
        F.nth_value("requester_id", 2).over(w_channel_csat_by_requester_id_3).alias("second_requester_id"),
        F.size(F.collect_set("channel").over(w_agent_id_priority)).alias("distinct_channel_in_agent_id_priority"),
        F.round(F.avg("csat").over(w_agent_id_channel), 2).alias("agent_id_channel_avg_csat"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "closed_at",
        "priority",
        "csat",
        "requester_id_sum_2",
        "last_closed_at",
        "csat_avg_3_prior",
        "requester_id_cume_dist",
        "agent_id_priority_avg_csat",
        "agent_id_priority_min_requester_id",
        "csat_sum_2_prior",
        "second_requester_id",
        "distinct_channel_in_agent_id_priority",
        "agent_id_channel_avg_csat",
        F.min("csat_avg_3_prior").over(w_seq_no_last_closed_at_year).alias("seq_no_last_closed_at_year_min_csat_avg_3_prior"),
        F.round(F.percent_rank().over(w_all_by_requester_id_ties), 3).alias("requester_id_percent_rank"),
        F.round(F.col("csat") - F.avg("csat").over(w_seq_no_last_closed_at_year), 2).alias("csat_vs_seq_no_last_closed_at_year_avg"),
        F.row_number().over(w_seq_no_agent_id_by_requester).alias("seq_no_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "priority",
        "csat",
        "requester_id_sum_2",
        "last_closed_at",
        "csat_avg_3_prior",
        "requester_id_cume_dist",
        "agent_id_priority_avg_csat",
        "agent_id_priority_min_requester_id",
        "csat_sum_2_prior",
        "second_requester_id",
        "distinct_channel_in_agent_id_priority",
        "agent_id_channel_avg_csat",
        "seq_no_last_closed_at_year_min_csat_avg_3_prior",
        "requester_id_percent_rank",
        "csat_vs_seq_no_last_closed_at_year_avg",
        "seq_no_2",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
    .orderBy(
        "priority",
        F.col("seq_no_last_closed_at_year_min_csat_avg_3_prior").asc_nulls_last(),
    )
)
