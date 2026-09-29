from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_all_by_minutes_since_prev_ev = Window.orderBy("minutes_since_prev_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_minutes_since_prev_ev_2 = Window.orderBy("minutes_since_prev_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_percent_rank = Window.orderBy(F.col("event_at_percent_rank").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_all_by_minutes_since_prev_ev_3 = Window.orderBy("minutes_since_prev_event_at", "id").rowsBetween(-3, -1)
w_all_by_minutes_since_prev_ev_4 = Window.orderBy(F.col("minutes_since_prev_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_known_event_at_c = Window.orderBy(F.col("last_known_event_at_cume_dist_2").desc())
w_all_by_minutes_since_prev_ev_5 = Window.orderBy(F.col("minutes_since_prev_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_trends = (
    valid
    .select(
        "id",
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at), 3).alias("event_at_cume_dist_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "minutes_since_prev_event_at",
        F.last("event_at_cume_dist_2", ignorenulls=True).over(w_all_by_minutes_since_prev_ev).alias("last_known_event_at_cume_dist_2"),
        F.sum("event_at_cume_dist_2").over(w_all_by_minutes_since_prev_ev).alias("running_event_at_cume_dist_2"),
        F.first("minutes_since_prev_event_at").over(w_all_by_minutes_since_prev_ev_2).alias("first_minutes_since_prev_event_at"),
        F.round(F.avg("event_at_cume_dist_2").over(w_all_by_event_at_percent_rank), 2).alias("event_at_cume_dist_2_avg_6_prior"),
        F.sum("event_at_percent_rank").over(w_all_by_minutes_since_prev_ev_3).alias("event_at_percent_rank_sum_3_prior"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.last("minutes_since_prev_event_at", ignorenulls=True).over(w_all_by_minutes_since_prev_ev_4).alias("last_known_minutes_since_prev_event_at"),
        F.round(F.cume_dist().over(w_all_by_last_known_event_at_c), 3).alias("last_known_event_at_cume_dist_2_cume_dist"),
        F.first("minutes_since_prev_event_at", ignorenulls=True).over(w_all_by_minutes_since_prev_ev_4).alias("first_known_minutes_since_prev_event_at"),
        F.nth_value("minutes_since_prev_event_at", 2).over(w_all_by_minutes_since_prev_ev_5).alias("second_minutes_since_prev_event_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "minutes_since_prev_event_at",
        "last_known_event_at_cume_dist_2",
        "running_event_at_cume_dist_2",
        "first_minutes_since_prev_event_at",
        "event_at_percent_rank_sum_3_prior",
        "last_known_minutes_since_prev_event_at",
        "last_known_event_at_cume_dist_2_cume_dist",
        "first_known_minutes_since_prev_event_at",
    )
    .orderBy("running_event_at_cume_dist_2")
)
