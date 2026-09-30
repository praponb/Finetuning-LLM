from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_csat_channel_by_priority_len = Window.partitionBy("csat", "channel").orderBy(F.col("priority_len").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_opened_at_month_by_opened_at = Window.partitionBy(F.date_trunc("month", "opened_at")).orderBy("opened_at", "id")
w_csat_priority_len = Window.partitionBy("csat", "priority_len")
w_opened_at_month_by_opened_at_2 = Window.partitionBy(F.date_trunc("month", "opened_at")).orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_agent_id_by_opened = Window.partitionBy("priority", "agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy(F.col("csat").desc_nulls_first(), "id")
w_agent_id_by_csat_full = Window.partitionBy("agent_id").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_csat_moving_6 = Window.partitionBy("agent_id").orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_priority_len_by_priority_len = Window.partitionBy("priority_len").orderBy(F.col("priority_len_max_3").desc(), "id")
w_channel = Window.partitionBy("channel")
w_priority_csat_by_csat_priori = Window.partitionBy("priority", "csat").orderBy("csat_priority_len_avg_requester_id", "id")
w_distinct_agent_id_in_priorit = Window.partitionBy("distinct_agent_id_in_priority", "prev3_priority_len")

recent = (
    df_tickets
    .filter((F.month(F.col("opened_at")).isin(1, 3)) & (F.col("agent_id").isNotNull()))
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
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_priority), F.lit(0)), 4).alias("csat_share_of_priority"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_priority), 2).alias("requester_id_vs_priority_avg"),
        F.max("priority_len").over(w_csat_channel_by_priority_len).alias("priority_len_max_3"),
        F.lag("priority_len", 3, 0).over(w_opened_at_month_by_opened_at).alias("prev3_priority_len"),
        F.max("requester_id").over(w_csat_priority_len).alias("csat_priority_len_max_requester_id"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_csat_priority_len), 2).alias("requester_id_vs_csat_priority_len_avg"),
        F.first("opened_at", ignorenulls=True).over(w_opened_at_month_by_opened_at_2).alias("first_known_opened_at"),
        F.round(F.avg("requester_id").over(w_csat_priority_len), 2).alias("csat_priority_len_avg_requester_id"),
        F.nth_value("opened_at", 2).over(w_priority_agent_id_by_opened).alias("second_opened_at"),
        F.lead("opened_at").over(w_agent_id_by_csat).alias("next_opened_at"),
        F.max("requester_id").over(w_priority).alias("priority_max_requester_id"),
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
        F.nth_value("opened_at", 2).over(w_agent_id_by_csat_full).alias("second_opened_at_2"),
        F.round(F.avg("csat").over(w_agent_id_by_csat_moving_6), 2).alias("csat_avg_7"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.dense_rank().over(w_priority_len_by_priority_len).alias("priority_len_max_3_dense_rank"),
        F.max("prev3_priority_len").over(w_channel).alias("channel_max_prev3_priority_len"),
        F.lag("second_opened_at_2", 2).over(w_priority_csat_by_csat_priori).alias("prev2_second_opened_at_2"),
        F.sum("priority_len_max_3").over(w_distinct_agent_id_in_priorit).alias("distinct_agent_id_in_priority_prev3_priority_len"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "priority_len",
        "csat_share_of_priority",
        "requester_id_vs_priority_avg",
        "priority_len_max_3",
        "first_known_opened_at",
        "csat_priority_len_avg_requester_id",
        "next_opened_at",
        "second_opened_at_2",
        "csat_avg_7",
        "channel_max_prev3_priority_len",
        "distinct_agent_id_in_priority_prev3_priority_len",
        "rows_in_channel",
    )
    .orderBy(
        F.col("csat_avg_7").desc(),
        F.col("priority_len_max_3").desc_nulls_first(),
    )
)
