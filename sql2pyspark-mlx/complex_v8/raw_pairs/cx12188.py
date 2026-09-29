from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_priority = Window.partitionBy("agent_id", "priority")
w_closed_at_month_priority = Window.partitionBy(F.month("closed_at"), "priority")
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").desc(), "id")
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id")
w_priority_len_agent_id_by_csa = Window.partitionBy("priority_len", "agent_id").orderBy(F.col("csat").desc(), "id").rowsBetween(-1, -1)
w_priority_len_agent_id_by_csa_2 = Window.partitionBy("priority_len", "agent_id").orderBy(F.col("csat").desc(), "id")
w_all_by_opened_at_running = Window.orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_len_by_opened_at_ti = Window.partitionBy("priority_len").orderBy("opened_at")
w_agent_id_by_closed_at_prior = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(-6, -1)
w_priority_len_by_opened_at = Window.partitionBy("priority_len").orderBy("opened_at", "id")
w_priority_len_agent_id_by_csa_3 = Window.partitionBy("priority_len", "agent_id").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "refund"))
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
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "priority",
        "priority_len",
        "channel_or_unknown",
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_priority), 2).alias("csat_vs_agent_id_priority_avg"),
        F.round(F.col("priority_len") / F.nullif(F.sum("priority_len").over(w_closed_at_month_priority), F.lit(0)), 4).alias("priority_len_share_of_closed_at_month_priority"),
        F.sum("priority_len").over(w_closed_at_month_priority).alias("closed_at_month_priority_sum_priority_len"),
        F.rank().over(w_agent_id_by_opened_at).alias("opened_at_rank"),
        F.first("csat").over(w_all_by_opened_at_full).alias("first_csat"),
        F.row_number().over(w_agent_id_by_closed_at).alias("seq_no"),
        F.sum("priority_len").over(w_priority_len_agent_id_by_csa).alias("priority_len_sum_1_prior"),
        F.lead("closed_at").over(w_priority_len_agent_id_by_csa_2).alias("next_closed_at"),
        F.sum("priority_len").over(w_all_by_opened_at_running).alias("running_priority_len"),
        F.size(F.collect_set("channel_or_unknown").over(w_agent_id_priority)).alias("distinct_channel_or_unknown_in_agent_id_priority"),
        F.dense_rank().over(w_priority_len_by_opened_at_ti).alias("opened_at_dense_rank"),
        F.round(F.avg("csat").over(w_agent_id_by_closed_at_prior), 2).alias("csat_avg_6_prior"),
        F.round(F.cume_dist().over(w_priority_len_by_opened_at), 3).alias("opened_at_cume_dist"),
        F.nth_value("requester_id", 2).over(w_priority_len_agent_id_by_csa_3).alias("second_requester_id"),
    )
)

result = (
    metrics
    .select(
        "id",
        "priority",
        "priority_len",
        "channel_or_unknown",
        "seq_no",
        "priority_len_sum_1_prior",
        "next_closed_at",
        "csat_avg_6_prior",
        "opened_at_cume_dist",
        "second_requester_id",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("priority_len").desc())
)
