from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_id = Window.partitionBy("channel", "agent_id")
w_opened_at_month_priority_by = Window.partitionBy(F.month("opened_at"), "priority").orderBy(F.col("requester_id_filled").asc_nulls_last(), "id")
w_channel_priority_by_requeste = Window.partitionBy("channel", "priority").orderBy("requester_id", "id")
w_agent_id = Window.partitionBy("agent_id")
w_priority_by_requester_id_fil = Window.partitionBy("priority").orderBy("requester_id_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_requester_id_fil_2 = Window.partitionBy("priority").orderBy("requester_id_filled")
w_channel_priority_by_requeste_2 = Window.partitionBy("channel", "priority").orderBy("requester_id", "id").rowsBetween(-2, Window.currentRow)
w_priority_by_requester_id_fil_3 = Window.partitionBy("priority").orderBy("requester_id_filled", "id")
w_all_by_distinct_channel_in_a = Window.orderBy("distinct_channel_in_agent_id", "id").rowsBetween(-3, Window.currentRow)
w_all_by_distinct_channel_in_a_2 = Window.orderBy("distinct_channel_in_agent_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_requester_id_filled_rank = Window.partitionBy("requester_id_filled_rank")

base_tickets = (
    df_tickets
    .filter(~F.col("priority").isin("P1", "P4"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        F.concat_ws(": ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

with_windows = (
    base_tickets
    .select(
        "*",
        F.size(F.collect_set("priority").over(w_channel_agent_id)).alias("distinct_priority_in_channel_agent_id"),
        F.round(F.percent_rank().over(w_opened_at_month_priority_by), 3).alias("requester_id_filled_percent_rank"),
        F.row_number().over(w_channel_priority_by_requeste).alias("seq_no"),
        F.round(F.col("requester_id_filled") - F.avg("requester_id_filled").over(w_agent_id), 2).alias("requester_id_filled_vs_agent_id_avg"),
        F.count(F.lit(1)).over(w_priority_by_requester_id_fil).alias("running_count"),
        F.size(F.collect_set("channel").over(w_agent_id)).alias("distinct_channel_in_agent_id"),
        F.rank().over(w_priority_by_requester_id_fil_2).alias("requester_id_filled_rank"),
        F.sum("requester_id").over(w_channel_priority_by_requeste_2).alias("requester_id_sum_3"),
        F.ntile(5).over(w_channel_priority_by_requeste).alias("requester_id_quintile"),
        (F.col("requester_id") - F.lag("requester_id").over(w_priority_by_requester_id_fil_3)).alias("requester_id_change"),
        F.row_number().over(w_opened_at_month_priority_by).alias("rn"),
        F.sum("requester_id_filled").over(w_agent_id).alias("agent_id_sum_requester_id_filled"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "priority_channel_label",
        "requester_id_filled",
        "requester_id_filled_percent_rank",
        "seq_no",
        "running_count",
        "distinct_channel_in_agent_id",
        "requester_id_filled_rank",
        "requester_id_sum_3",
        F.round(F.avg("requester_id").over(w_all_by_distinct_channel_in_a), 2).alias("requester_id_avg_4"),
        F.sum("requester_id_sum_3").over(w_all_by_distinct_channel_in_a_2).alias("running_requester_id_sum_3"),
        F.sum("distinct_channel_in_agent_id").over(w_requester_id_filled_rank).alias("requester_id_filled_rank_sum_distinct_channel_in"),
        F.size(F.collect_set("agent_id").over(w_requester_id_filled_rank)).alias("distinct_agent_id_in_requester_id_filled_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "priority_channel_label",
        "requester_id_filled",
        "requester_id_filled_percent_rank",
        "seq_no",
        "running_count",
        "distinct_channel_in_agent_id",
        "requester_id_filled_rank",
        "running_requester_id_sum_3",
        "requester_id_filled_rank_sum_distinct_channel_in",
        "distinct_agent_id_in_requester_id_filled_rank",
        F.when(F.col("channel").isin("phone", "email"), "group_a").when(F.col("channel") == "chat", "group_b").otherwise("other").alias("channel_group"),
        (F.col("requester_id") - F.col("requester_id_filled")).alias("requester_id_minus_requester_id_filled"),
    )
)
