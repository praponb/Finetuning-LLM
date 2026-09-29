from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_id = Window.partitionBy("channel", "agent_id")
w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id")
w_priority_by_csat_ties = Window.partitionBy("priority").orderBy(F.col("csat").desc_nulls_first())
w_closed_at_to_date_by_closed = Window.partitionBy(F.to_date("closed_at")).orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_by_csat_full = Window.partitionBy("priority").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_closed_at_full = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_csat = Window.partitionBy("priority", "csat")
w_last_csat_by_closed_at = Window.partitionBy("last_csat").orderBy("closed_at", "id")
w_last_csat_by_closed_at_ties = Window.partitionBy("last_csat").orderBy("closed_at")
w_csat_by_csat_dense_rank = Window.partitionBy("csat").orderBy("csat_dense_rank", "id")
w_last_csat_by_closed_at_full = Window.partitionBy("last_csat").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_month_channel_by_c = Window.partitionBy(F.month("closed_at"), "channel").orderBy("closed_at", "id")

recent_tickets = df_tickets.filter(F.col("agent_id").isNotNull())

windowed = (
    recent_tickets
    .select(
        "*",
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel_agent_id), F.lit(0)), 4).alias("requester_id_share_of_channel_agent_id"),
        F.sum("requester_id").over(w_channel_agent_id).alias("channel_agent_id_sum_requester_id"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_channel_agent_id), F.lit(0)), 4).alias("csat_share_of_channel_agent_id"),
        F.row_number().over(w_agent_id_by_closed_at).alias("closed_at_seq"),
        F.dense_rank().over(w_priority_by_csat_ties).alias("csat_dense_rank"),
        F.first("closed_at").over(w_closed_at_to_date_by_closed).alias("first_closed_at"),
        F.nth_value("requester_id", 2).over(w_priority_by_csat_full).alias("second_requester_id"),
        F.count(F.lit(1)).over(w_channel_agent_id).alias("rows_in_channel_agent_id"),
        F.last("csat").over(w_agent_id_by_closed_at_full).alias("last_csat"),
        F.round(F.percent_rank().over(w_agent_id_by_closed_at), 3).alias("closed_at_percent_rank"),
        F.first("requester_id").over(w_priority_by_csat_full).alias("first_requester_id"),
    )
)

top_rows = windowed.filter(F.col("csat_dense_rank") == 1)

sequenced = (
    top_rows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "channel_agent_id_sum_requester_id",
        "csat_share_of_channel_agent_id",
        "csat_dense_rank",
        "first_closed_at",
        "second_requester_id",
        "rows_in_channel_agent_id",
        "last_csat",
        "first_requester_id",
        F.count(F.lit(1)).over(w_priority_csat).alias("rows_in_priority_csat"),
        F.lag("rows_in_channel_agent_id", 2).over(w_last_csat_by_closed_at).alias("prev2_rows_in_channel_agent_id"),
        F.round(F.percent_rank().over(w_last_csat_by_closed_at_ties), 3).alias("closed_at_percent_rank_2"),
        (F.col("closed_at_seq") - F.lag("closed_at_seq").over(w_csat_by_csat_dense_rank)).alias("closed_at_seq_change"),
        F.max("closed_at_seq").over(w_priority_csat).alias("priority_csat_max_closed_at_seq"),
        F.first("requester_id").over(w_last_csat_by_closed_at_full).alias("first_requester_id_2"),
        F.size(F.collect_set("agent_id").over(w_priority_csat)).alias("distinct_agent_id_in_priority_csat"),
        (F.col("csat") - F.lag("csat").over(w_closed_at_month_channel_by_c)).alias("csat_change"),
    )
)

result = (
    sequenced
    .filter(F.col("last_csat").between(1, 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "csat_share_of_channel_agent_id",
        "csat_dense_rank",
        "last_csat",
        "first_requester_id",
        "prev2_rows_in_channel_agent_id",
        "closed_at_percent_rank_2",
        "closed_at_seq_change",
        "priority_csat_max_closed_at_seq",
        "first_requester_id_2",
        "distinct_agent_id_in_priority_csat",
        "csat_change",
    )
    .orderBy(F.col("closed_at_percent_rank_2").asc_nulls_last())
)
