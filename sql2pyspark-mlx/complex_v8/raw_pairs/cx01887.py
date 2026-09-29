from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_minutes_since_prev_ev = Window.orderBy("minutes_since_prev_event_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_minutes_since_prev_ev_2 = Window.orderBy("minutes_since_prev_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_second_event_at_t = Window.partitionBy("event_type", F.to_date("second_event_at")).orderBy(F.col("second_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_event_at_2_to_date_ti = Window.partitionBy(F.to_date("second_event_at_2"), "ticket_id").orderBy(F.col("first_known_event_at_percent_rank_2").desc(), "id").rowsBetween(-6, -1)
w_second_event_at_2_month_by_s = Window.partitionBy(F.date_trunc("month", "second_event_at_2")).orderBy(F.col("second_event_at_2").desc(), "id")
w_all_by_minutes_since_prev_ev_3 = Window.orderBy("minutes_since_prev_event_at_max_3")
w_second_event_at_year = Window.partitionBy(F.year("second_event_at"))
w_second_event_at_2_to_date_ti_2 = Window.partitionBy(F.to_date("second_event_at_2"), "ticket_id").orderBy(F.col("first_known_event_at_percent_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 10) & (F.col("event_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")))
)

with_trends = (
    recent_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "second_event_at",
        "second_event_at_2",
        "minutes_since_prev_event_at",
        F.max("minutes_since_prev_event_at").over(w_all_by_minutes_since_prev_ev).alias("minutes_since_prev_event_at_max_3"),
        F.first("event_at_percent_rank", ignorenulls=True).over(w_all_by_minutes_since_prev_ev_2).alias("first_known_event_at_percent_rank"),
        F.first("event_at_percent_rank", ignorenulls=True).over(w_event_type_second_event_at_t).alias("first_known_event_at_percent_rank_2"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.sum("minutes_since_prev_event_at").over(w_second_event_at_2_to_date_ti).alias("minutes_since_prev_event_at_sum_6_prior"),
        F.round((F.unix_timestamp("second_event_at") - F.unix_timestamp(F.lag("second_event_at").over(w_second_event_at_2_month_by_s))) / 60, 1).alias("minutes_since_prev_second_event_at"),
        F.rank().over(w_all_by_minutes_since_prev_ev_3).alias("minutes_since_prev_event_at_max_3_rank"),
        F.round(F.avg("minutes_since_prev_event_at").over(w_second_event_at_year), 2).alias("second_event_at_year_avg_minutes_since_prev_even"),
        F.first("second_event_at_2").over(w_second_event_at_2_to_date_ti_2).alias("first_second_event_at_2"),
    )
)

result = (
    metrics
    .rollup("ticket_id", "minutes_since_prev_event_at_max_3_rank")
    .agg(
        F.round(F.avg("minutes_since_prev_event_at_sum_6_prior"), 2).alias("avg_minutes_since_prev_event_at_sum_6_prior"),
        F.max("minutes_since_prev_event_at").alias("max_minutes_since_prev_event_at"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("minutes_since_prev_event_at_max_3_rank").asc_nulls_last(),
    )
)
