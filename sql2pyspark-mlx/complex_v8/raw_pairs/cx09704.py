from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_moving_2 = Window.orderBy("event_at", "id").rowsBetween(-2, Window.currentRow)
w_ticket_id = Window.partitionBy("ticket_id")
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month = Window.partitionBy(F.month("event_at"))
w_event_type = Window.partitionBy("event_type")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_type_len_moving = Window.orderBy(F.col("event_type_len").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_distinct_event_type_in_event = Window.partitionBy("distinct_event_type_in_event_at_month")
w_ticket_id_first_event_at_mon = Window.partitionBy("ticket_id", F.date_trunc("month", "first_event_at")).orderBy(F.col("event_at_decile").desc(), "id")
w_all_by_event_type_len = Window.orderBy(F.col("event_type_len").asc_nulls_last(), "id")

prepared_events = (
    df_ticket_events
    .filter(F.col("event_type") != "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

sequenced = (
    prepared_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_len",
        F.round(F.avg("event_type_len").over(w_all_by_event_at_moving_2), 2).alias("event_type_len_avg_3"),
        F.size(F.collect_set("actor").over(w_ticket_id)).alias("distinct_actor_in_ticket_id"),
        F.nth_value("event_type_len", 2).over(w_ticket_id_actor_by_event_at).alias("second_event_type_len"),
        F.min("event_type_len").over(w_event_at_month).alias("event_at_month_min_event_type_len"),
        F.size(F.collect_set("event_type").over(w_ticket_id)).alias("distinct_event_type_in_ticket_id"),
        F.sum("event_type_len").over(w_event_type).alias("event_type_sum_event_type_len"),
        F.round(F.col("event_type_len") - F.avg("event_type_len").over(w_ticket_id), 2).alias("event_type_len_vs_ticket_id_avg"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.size(F.collect_set("event_type").over(w_event_at_month)).alias("distinct_event_type_in_event_at_month"),
    )
)

with_windows = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_len",
        "event_type_len_avg_3",
        "distinct_actor_in_ticket_id",
        "event_at_month_min_event_type_len",
        "distinct_event_type_in_ticket_id",
        "event_type_sum_event_type_len",
        "event_type_len_vs_ticket_id_avg",
        "first_event_at",
        "event_at_decile",
        "distinct_event_type_in_event_at_month",
        F.round(F.avg("event_type_len").over(w_all_by_event_type_len_moving), 2).alias("event_type_len_avg_7"),
        F.round(F.avg("event_at_decile").over(w_distinct_event_type_in_event), 2).alias("distinct_event_type_in_event_at_month_avg_event_"),
        F.row_number().over(w_ticket_id_first_event_at_mon).alias("event_at_decile_seq"),
        F.round(F.cume_dist().over(w_all_by_event_type_len), 3).alias("event_type_len_cume_dist"),
        F.count(F.lit(1)).over(w_ticket_id).alias("rows_in_ticket_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "actor",
        "event_type_len",
        "event_type_len_avg_3",
        "distinct_actor_in_ticket_id",
        "event_at_month_min_event_type_len",
        "distinct_event_type_in_ticket_id",
        "event_type_sum_event_type_len",
        "event_type_len_vs_ticket_id_avg",
        "first_event_at",
        "event_at_decile",
        "distinct_event_type_in_event_at_month",
        "event_type_len_avg_7",
        "distinct_event_type_in_event_at_month_avg_event_",
        "event_at_decile_seq",
        "rows_in_ticket_id",
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
    )
    .orderBy(F.col("event_type_sum_event_type_len").desc_nulls_first())
)
