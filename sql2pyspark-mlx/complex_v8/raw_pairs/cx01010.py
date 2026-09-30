from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at_dayofweek_r = Window.orderBy(F.col("closed_at_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at_dayofweek_f = Window.orderBy(F.col("closed_at_dayofweek").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_agent_id_by_request = Window.partitionBy("priority", "agent_id").orderBy(F.col("requester_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_agent_id_by_request_2 = Window.partitionBy("priority", "agent_id").orderBy(F.col("requester_id").desc_nulls_first(), "id")
w_closed_at_dayofweek_channel = Window.partitionBy("closed_at_dayofweek", "channel")
w_channel_by_closed_at_dayofwe = Window.partitionBy("channel").orderBy("closed_at_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_dayofweek = Window.partitionBy("closed_at_dayofweek")
w_all_by_closed_at_dayofweek_t = Window.orderBy("closed_at_dayofweek")
w_all_by_closed_at_dayofweek = Window.orderBy(F.col("closed_at_dayofweek").desc(), "id")
w_priority_closed_at_dayofweek = Window.partitionBy("priority", "closed_at_dayofweek").orderBy("second_requester_id")
w_distinct_priority_in_closed = Window.partitionBy("distinct_priority_in_closed_at_dayofweek")

recent_tickets = (
    df_tickets
    .filter(F.col("requester_id") < 18)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
    )
)

metrics = (
    recent_tickets
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "channel",
        "closed_at_dayofweek",
        F.last("closed_at_dayofweek", ignorenulls=True).over(w_all_by_closed_at_dayofweek_r).alias("last_known_closed_at_dayofweek"),
        F.nth_value("requester_id", 2).over(w_all_by_closed_at_dayofweek_f).alias("second_requester_id"),
        F.last("closed_at_dayofweek", ignorenulls=True).over(w_priority_agent_id_by_request).alias("last_known_closed_at_dayofweek_2"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_priority_agent_id_by_request_2))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.count(F.lit(1)).over(w_closed_at_dayofweek_channel).alias("rows_in_closed_at_dayofweek_channel"),
        F.nth_value("requester_id", 2).over(w_channel_by_closed_at_dayofwe).alias("second_requester_id_2"),
        F.size(F.collect_set("priority").over(w_closed_at_dayofweek)).alias("distinct_priority_in_closed_at_dayofweek"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_closed_at_dayofweek), F.lit(0)), 4).alias("csat_share_of_closed_at_dayofweek"),
        F.lag("requester_id", 2).over(w_priority_agent_id_by_request_2).alias("prev2_requester_id"),
        F.dense_rank().over(w_all_by_closed_at_dayofweek_t).alias("closed_at_dayofweek_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_closed_at_dayofweek), 3).alias("closed_at_dayofweek_percent_rank"),
        F.lead("closed_at_dayofweek", 3).over(w_priority_agent_id_by_request_2).alias("next3_closed_at_dayofweek"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.round(F.percent_rank().over(w_priority_closed_at_dayofweek), 3).alias("second_requester_id_percent_rank"),
        F.round(F.col("prev2_requester_id") / F.nullif(F.sum("prev2_requester_id").over(w_distinct_priority_in_closed), F.lit(0)), 4).alias("prev2_requester_id_share_of_distinct_priority_in"),
        F.min("closed_at_dayofweek").over(w_distinct_priority_in_closed).alias("distinct_priority_in_closed_at_dayofweek_min_clo"),
        F.count(F.lit(1)).over(w_distinct_priority_in_closed).alias("rows_in_distinct_priority_in_closed_at_dayofweek"),
    )
)

result = (
    windowed
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "channel",
        "closed_at_dayofweek",
        "last_known_closed_at_dayofweek",
        "rows_in_closed_at_dayofweek_channel",
        "second_requester_id_2",
        "distinct_priority_in_closed_at_dayofweek",
        "prev2_requester_id",
        "closed_at_dayofweek_dense_rank",
        "closed_at_dayofweek_percent_rank",
        "second_requester_id_percent_rank",
        "prev2_requester_id_share_of_distinct_priority_in",
        "distinct_priority_in_closed_at_dayofweek_min_clo",
        "rows_in_distinct_priority_in_closed_at_dayofweek",
    )
    .orderBy(F.col("prev2_requester_id").desc())
)
