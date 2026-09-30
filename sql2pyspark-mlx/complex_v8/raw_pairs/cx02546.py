from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_priority_by_closed_a = Window.partitionBy("channel", "priority").orderBy("closed_at_week")
w_priority = Window.partitionBy("priority")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_channel_priority_by_closed_a_2 = Window.partitionBy("channel", "priority").orderBy("closed_at_week", "id")
w_priority_agent_id_by_agent_h = Window.partitionBy("priority", "agent_id").orderBy("agent_hired_on", "id")
w_channel_priority_by_closed_a_3 = Window.partitionBy("channel", "priority").orderBy("closed_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_hired_on_month = Window.partitionBy(F.date_trunc("month", "agent_hired_on"))
w_requester_id_tercile_channel = Window.partitionBy("requester_id_tercile", "channel").orderBy("distinct_agent_id_in_priority")
w_all_by_next3_requester_id = Window.orderBy("next3_requester_id", "id")
w_requester_id_tercile_channel_2 = Window.partitionBy("requester_id_tercile", "channel").orderBy("distinct_agent_id_in_priority", "id")
w_requester_id_dense_rank_by_r = Window.partitionBy("requester_id_dense_rank").orderBy(F.col("requester_id_vs_priority_avg").desc())
w_priority_requester_id_dense = Window.partitionBy("priority", "requester_id_dense_rank").orderBy(F.col("distinct_agent_id_in_priority").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_requester_id_dense_rank_requ = Window.partitionBy("requester_id_dense_rank", "requester_id_tercile")
w_all_by_next3_requester_id_ru = Window.orderBy("next3_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .filter(F.col("requester_id") <= 23)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        F.date_trunc("week", F.col("closed_at")).alias("closed_at_week"),
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.closed_at_week",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.rank().over(w_channel_priority_by_closed_a).alias("closed_at_week_rank"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_priority), F.lit(0)), 4).alias("requester_id_share_of_priority"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_priority), 2).alias("requester_id_vs_priority_avg"),
        F.lead("requester_id", 3, 0).over(w_all_by_requester_id).alias("next3_requester_id"),
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
        F.round(F.avg("requester_id").over(w_priority), 2).alias("priority_avg_requester_id"),
        F.dense_rank().over(w_all_by_requester_id).alias("requester_id_dense_rank"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_channel_priority_by_closed_a_2)).alias("days_since_prev_agent_hired_on"),
        F.lag("requester_id").over(w_priority_agent_id_by_agent_h).alias("prev_requester_id"),
        F.nth_value("requester_id", 2).over(w_channel_priority_by_closed_a_3).alias("second_requester_id"),
        F.ntile(3).over(w_all_by_requester_id).alias("requester_id_tercile"),
        F.lead("closed_at_week", 2).over(w_all_by_requester_id).alias("next2_closed_at_week"),
        F.lead("closed_at_week", 3).over(w_all_by_requester_id).alias("next3_closed_at_week"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "priority",
        "channel",
        "closed_at_week",
        "agent_hired_on",
        "agent_name",
        "closed_at_week_rank",
        "next3_requester_id",
        "days_since_prev_agent_hired_on",
        "prev_requester_id",
        "requester_id_tercile",
        "next3_closed_at_week",
        F.size(F.collect_set("agent_id").over(w_agent_hired_on_month)).alias("distinct_agent_id_in_agent_hired_on_month"),
        F.dense_rank().over(w_requester_id_tercile_channel).alias("distinct_agent_id_in_priority_dense_rank"),
        F.row_number().over(w_all_by_next3_requester_id).alias("rn"),
        F.lead("closed_at_week_rank", 3).over(w_requester_id_tercile_channel_2).alias("next3_closed_at_week_rank"),
        F.round(F.cume_dist().over(w_requester_id_dense_rank_by_r), 3).alias("requester_id_vs_priority_avg_cume_dist"),
        F.round(F.avg("requester_id_tercile").over(w_priority_requester_id_dense), 2).alias("requester_id_tercile_avg_3"),
        F.count(F.lit(1)).over(w_requester_id_dense_rank_requ).alias("rows_in_requester_id_dense_rank_requester_id_ter"),
        F.last("requester_id_dense_rank", ignorenulls=True).over(w_all_by_next3_requester_id_ru).alias("last_known_requester_id_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "priority",
        "channel",
        "agent_hired_on",
        "agent_name",
        "closed_at_week_rank",
        "days_since_prev_agent_hired_on",
        "prev_requester_id",
        "requester_id_tercile",
        "next3_closed_at_week",
        "distinct_agent_id_in_agent_hired_on_month",
        "distinct_agent_id_in_priority_dense_rank",
        "rn",
        "requester_id_vs_priority_avg_cume_dist",
        "requester_id_tercile_avg_3",
        "rows_in_requester_id_dense_rank_requester_id_ter",
        "last_known_requester_id_dense_rank",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.when(F.col("requester_id_tercile") >= 7, "large").when(F.col("requester_id_tercile") >= 4, "medium").otherwise("small").alias("requester_id_tercile_tier"),
    )
)
