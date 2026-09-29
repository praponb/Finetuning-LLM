from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority")
w_opened_at_month = Window.partitionBy(F.month("opened_at"))
w_all_by_closed_at_moving_6 = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_agent_id_by_minutes_closed_a = Window.partitionBy("agent_id").orderBy("minutes_closed_at_to_opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_agent_id_by_request = Window.partitionBy("priority", "agent_id").orderBy("requester_id", "id").rowsBetween(-2, -1)
w_priority_agent_id_by_request_2 = Window.partitionBy("priority", "agent_id").orderBy("requester_id", "id").rowsBetween(-3, Window.currentRow)
w_all_by_closed_at_full = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_requester_id_dis = Window.partitionBy("agent_id").orderBy(F.col("requester_id_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .filter((F.col("requester_id") <= 22) & (F.col("closed_at") < F.lit("2025-02-21 07:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
        F.abs(F.col("requester_id") - 18).alias("requester_id_distance"),
    )
)

windowed = (
    cleaned_tickets
    .select(
        "*",
        F.max("requester_id").over(w_priority).alias("priority_max_requester_id"),
        F.size(F.collect_set("agent_id").over(w_opened_at_month)).alias("distinct_agent_id_in_opened_at_month"),
        F.max("requester_id").over(w_all_by_closed_at_moving_6).alias("requester_id_max_7"),
        F.size(F.collect_set("agent_id").over(w_priority)).alias("distinct_agent_id_in_priority"),
        F.round(F.col("requester_id_distance") / F.nullif(F.sum("requester_id_distance").over(w_priority), F.lit(0)), 4).alias("requester_id_distance_share_of_priority"),
        F.sum("requester_id_distance").over(w_opened_at_month).alias("opened_at_month_sum_requester_id_distance"),
        F.min("requester_id").over(w_agent_id_by_minutes_closed_a).alias("min_requester_id_so_far"),
        F.sum("requester_id").over(w_opened_at_month).alias("opened_at_month_sum_requester_id"),
        F.min("requester_id_distance").over(w_opened_at_month).alias("opened_at_month_min_requester_id_distance"),
        F.round(F.avg("requester_id_distance").over(w_priority_agent_id_by_request), 2).alias("requester_id_distance_avg_2_prior"),
        F.round(F.avg("requester_id").over(w_priority_agent_id_by_request_2), 2).alias("requester_id_avg_4"),
        F.nth_value("requester_id", 2).over(w_all_by_closed_at_full).alias("second_requester_id"),
        F.sum("requester_id").over(w_agent_id_by_requester_id_dis).alias("running_requester_id"),
    )
)

result = (
    windowed
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "tags",
        "minutes_closed_at_to_opened_at",
        "distinct_agent_id_in_opened_at_month",
        "requester_id_max_7",
        "distinct_agent_id_in_priority",
        "requester_id_distance_share_of_priority",
        "min_requester_id_so_far",
        "opened_at_month_sum_requester_id",
        "opened_at_month_min_requester_id_distance",
        "second_requester_id",
        "running_requester_id",
        F.array_contains(F.col("tags"), "bug").alias("has_bug"),
        F.concat(F.col("priority"), F.lit("#"), F.col("second_requester_id").cast("string")).alias("priority_ref"),
    )
    .orderBy(
        F.col("requester_id_distance_share_of_priority").desc(),
        F.col("priority_ref").asc(),
    )
)
