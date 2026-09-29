from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_by_requester_id_runn = Window.partitionBy("channel").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_year_agent_id = Window.partitionBy(F.year("closed_at"), "agent_id")
w_channel_by_requester_id_full = Window.partitionBy("channel").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_or_unknown_priority = Window.partitionBy("priority_or_unknown", "priority").orderBy("requester_id", "id")
w_channel_by_requester_id = Window.partitionBy("channel").orderBy("requester_id", "id")
w_agent_id_by_next_requester_i = Window.partitionBy("agent_id").orderBy(F.col("next_requester_id").desc(), "id").rowsBetween(-2, Window.currentRow)
w_agent_id_by_next_requester_i_2 = Window.partitionBy("agent_id").orderBy(F.col("next_requester_id").desc(), "id").rowsBetween(-1, Window.currentRow)
w_next_closed_at_year_priority = Window.partitionBy(F.year("next_closed_at"), "priority_or_unknown").orderBy(F.col("requester_id_share_of_closed_at_year_agent_id").desc(), "id")
w_next_closed_at_month_agent_i = Window.partitionBy(F.date_trunc("month", "next_closed_at"), "agent_id")
w_next_closed_at_year_priority_2 = Window.partitionBy(F.year("next_closed_at"), "priority_or_unknown").orderBy(F.col("requester_id_share_of_closed_at_year_agent_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next_closed_at_year_priority_3 = Window.partitionBy(F.year("next_closed_at"), "priority_or_unknown").orderBy(F.col("requester_id_share_of_closed_at_year_agent_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_next_requester_i_3 = Window.partitionBy("agent_id").orderBy(F.col("next_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .filter((F.col("agent_id") >= 2) | (F.col("priority") == "P1"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "tags",
        F.when(F.col("requester_id") >= 18, "A").when(F.col("requester_id") >= 5, "B").otherwise("C").alias("requester_id_tier"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

metrics = (
    valid_tickets
    .select(
        "*",
        F.sum("requester_id").over(w_channel_by_requester_id_runn).alias("running_requester_id"),
        F.size(F.collect_set("priority_or_unknown").over(w_closed_at_year_agent_id)).alias("distinct_priority_or_unknown_in_closed_at_year_a"),
        F.nth_value("closed_at", 2).over(w_channel_by_requester_id_full).alias("second_closed_at"),
        F.lead("requester_id").over(w_priority_or_unknown_priority).alias("next_requester_id"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_closed_at_year_agent_id), F.lit(0)), 4).alias("requester_id_share_of_closed_at_year_agent_id"),
        F.lead("closed_at").over(w_channel_by_requester_id).alias("next_closed_at"),
        F.round(F.avg("requester_id").over(w_closed_at_year_agent_id), 2).alias("closed_at_year_agent_id_avg_requester_id"),
        F.min("requester_id").over(w_closed_at_year_agent_id).alias("closed_at_year_agent_id_min_requester_id"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "priority_or_unknown",
        "distinct_priority_or_unknown_in_closed_at_year_a",
        "second_closed_at",
        "next_requester_id",
        "next_closed_at",
        "closed_at_year_agent_id_min_requester_id",
        F.sum("next_requester_id").over(w_agent_id_by_next_requester_i).alias("next_requester_id_sum_3"),
        F.max("next_requester_id").over(w_agent_id_by_next_requester_i_2).alias("next_requester_id_max_2"),
        F.dense_rank().over(w_next_closed_at_year_priority).alias("requester_id_share_of_closed_at_year_agent_id_de"),
        F.count(F.lit(1)).over(w_next_closed_at_month_agent_i).alias("rows_in_next_closed_at_month_agent_id"),
        F.nth_value("running_requester_id", 2).over(w_next_closed_at_year_priority_2).alias("second_running_requester_id"),
        F.lead("next_requester_id", 1, 0).over(w_next_closed_at_year_priority).alias("next_next_requester_id"),
        F.sum("requester_id").over(w_next_closed_at_year_priority_3).alias("running_requester_id_2"),
        F.first("requester_id", ignorenulls=True).over(w_agent_id_by_next_requester_i_3).alias("first_known_requester_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "priority_or_unknown",
        "next_requester_id",
        "next_closed_at",
        "next_requester_id_sum_3",
        "next_requester_id_max_2",
        "requester_id_share_of_closed_at_year_agent_id_de",
        "rows_in_next_closed_at_month_agent_id",
        "next_next_requester_id",
        "first_known_requester_id",
    )
)
