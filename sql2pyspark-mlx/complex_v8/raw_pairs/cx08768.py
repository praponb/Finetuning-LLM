from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_priority_b = Window.partitionBy("event_type", "ticket_priority").orderBy(F.col("ticket_requester_id").desc())
w_ticket_channel_ticket_priori = Window.partitionBy("ticket_channel", "ticket_priority").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(-2, Window.currentRow)
w_ticket_channel_ticket_priori_2 = Window.partitionBy("ticket_channel", "ticket_priority").orderBy(F.col("ticket_requester_id").desc(), "id")
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at")
w_ticket_id_event_at_month_by = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at")).orderBy("event_at")
w_ticket_channel_ticket_id = Window.partitionBy("ticket_channel", "ticket_id")
w_event_type_ticket_priority_b_2 = Window.partitionBy("event_type", "ticket_priority").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_channel_ticket_priori_3 = Window.partitionBy("ticket_channel", "ticket_priority").orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_rank_event_type = Window.partitionBy("event_at_rank", "event_type")
w_event_type_by_distinct_event = Window.partitionBy("event_type").orderBy("distinct_event_type_in_ticket_channel_ticket_id")
w_all_by_event_at_percent_rank = Window.orderBy(F.col("event_at_percent_rank").desc(), "id")
w_distinct_event_type_in_ticke = Window.partitionBy("distinct_event_type_in_ticket_channel_ticket_id").orderBy(F.col("second_ticket_requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_distinct_event_2 = Window.partitionBy("event_type").orderBy("distinct_event_type_in_ticket_channel_ticket_id", "id")
w_distinct_event_type_in_ticke_2 = Window.partitionBy("distinct_event_type_in_ticket_channel_ticket_id", "event_at_rank")
w_distinct_event_type_in_ticke_3 = Window.partitionBy("distinct_event_type_in_ticket_channel_ticket_id").orderBy(F.col("second_ticket_requester_id").asc_nulls_last(), "id")

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp"))
)

with_parent_ticket = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

windowed = (
    with_parent_ticket
    .select(
        "id",
        "event_type",
        "ticket_requester_id",
        "ticket_priority",
        F.round(F.cume_dist().over(w_event_type_ticket_priority_b), 3).alias("ticket_requester_id_cume_dist"),
        F.round(F.avg("ticket_requester_id").over(w_ticket_channel_ticket_priori), 2).alias("ticket_requester_id_avg_3"),
        F.lead("ticket_requester_id").over(w_ticket_channel_ticket_priori_2).alias("next_ticket_requester_id"),
        F.round(F.cume_dist().over(w_event_type_ticket_id_by_even), 3).alias("event_at_cume_dist"),
        F.rank().over(w_event_type_ticket_id_by_even).alias("event_at_rank"),
        F.round(F.percent_rank().over(w_ticket_id_event_at_month_by), 3).alias("event_at_percent_rank"),
        F.size(F.collect_set("event_type").over(w_ticket_channel_ticket_id)).alias("distinct_event_type_in_ticket_channel_ticket_id"),
        F.nth_value("ticket_requester_id", 2).over(w_event_type_ticket_priority_b_2).alias("second_ticket_requester_id"),
        F.max("ticket_requester_id").over(w_ticket_channel_ticket_priori_3).alias("max_ticket_requester_id_so_far"),
    )
)

annotated = (
    windowed
    .filter(F.col("ticket_priority") != "P2")
    .select(
        "*",
        F.concat(F.col("ticket_priority"), F.lit("#"), F.col("next_ticket_requester_id").cast("string")).alias("ticket_priority_ref"),
        F.concat_ws(": ", F.col("event_type"), F.col("ticket_priority")).alias("event_type_ticket_priority_label"),
        F.concat(F.col("ticket_priority"), F.lit("#"), F.col("ticket_requester_id").cast("string")).alias("ticket_priority_ref_2"),
    )
)

with_windows = (
    annotated
    .select(
        "id",
        "event_type",
        "ticket_requester_id",
        "ticket_requester_id_cume_dist",
        "ticket_requester_id_avg_3",
        "next_ticket_requester_id",
        "event_at_cume_dist",
        "event_at_rank",
        "event_at_percent_rank",
        "second_ticket_requester_id",
        "max_ticket_requester_id_so_far",
        "event_type_ticket_priority_label",
        F.size(F.collect_set("ticket_priority").over(w_event_at_rank_event_type)).alias("distinct_ticket_priority_in_event_at_rank_event_"),
        F.round(F.percent_rank().over(w_event_type_by_distinct_event), 3).alias("distinct_event_type_in_ticket_channel_ticket_id_"),
        F.dense_rank().over(w_all_by_event_at_percent_rank).alias("event_at_percent_rank_dense_rank"),
        F.first("ticket_requester_id", ignorenulls=True).over(w_distinct_event_type_in_ticke).alias("first_known_ticket_requester_id"),
        F.round(F.percent_rank().over(w_event_type_by_distinct_event_2), 3).alias("distinct_event_type_in_ticket_channel_ticket_id__2"),
        F.count(F.lit(1)).over(w_distinct_event_type_in_ticke_2).alias("rows_in_distinct_event_type_in_ticket_channel_ti"),
        F.round(F.cume_dist().over(w_distinct_event_type_in_ticke_3), 3).alias("second_ticket_requester_id_cume_dist"),
    )
)

rollup_base = (
    with_windows
    .groupBy("event_at_rank")
    .agg(
        F.sum("event_at_percent_rank").alias("total_event_at_percent_rank"),
        F.sum("event_at_rank").alias("total_event_at_rank"),
        F.sum(F.when(F.col("event_type").isNotNull(), F.col("rows_in_distinct_event_type_in_ticket_channel_ti"))).alias("rows_in_distinct_event_type_in_ticket_channel_ti_2"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.sum("first_known_ticket_requester_id").alias("sum_first_known_ticket_requester_id_check"),
    )
    .filter(F.col("sum_first_known_ticket_requester_id_check") > 2)
    .drop("sum_first_known_ticket_requester_id_check")
)

result = (
    rollup_base
    .filter(F.col("total_event_at_percent_rank") < 5.4)
    .select(
        "event_at_rank",
        "total_event_at_percent_rank",
        "total_event_at_rank",
        "rows_in_distinct_event_type_in_ticket_channel_ti_2",
        (F.floor(F.col("event_at_rank") / 2) * 2).alias("event_at_rank_bucket"),
    )
)
