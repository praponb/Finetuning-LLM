from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_by_event_at_2 = Window.partitionBy(F.to_date("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_event_at_month = Window.partitionBy(F.month("first_event_at"))
w_event_at_quartile_by_last_ev = Window.partitionBy("event_at_quartile").orderBy(F.col("last_event_at").desc(), "id").rowsBetween(-2, -1)
w_all_by_event_at_quartile = Window.orderBy("event_at_quartile", "id")
w_event_at_quartile_by_last_ev_2 = Window.partitionBy("event_at_quartile").orderBy(F.col("last_event_at").desc(), "id")

recent_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 22) & (F.col("event_type") != "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

ranked = (
    recent_events
    .select(
        "*",
        F.dense_rank().over(w_ticket_id_by_event_at).alias("event_at_dense_rank"),
        F.rank().over(w_ticket_id_by_event_at_ties).alias("event_at_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(3).over(w_event_at_to_date_by_event_at).alias("event_at_tercile"),
        F.lag("event_at", 2).over(w_ticket_id_by_event_at).alias("prev2_event_at"),
        F.dense_rank().over(w_event_at_to_date_by_event_at).alias("event_at_dense_rank_2"),
        F.row_number().over(w_ticket_id_by_event_at).alias("seq_no"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist_2"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.row_number().over(w_all_by_event_at).alias("seq_no_2"),
        F.ntile(4).over(w_event_at_to_date_by_event_at).alias("event_at_quartile"),
        F.first("event_at").over(w_event_at_to_date_by_event_at_2).alias("first_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
    )
)

top_per_group = ranked.filter(F.col("event_at_dense_rank") == 1)

ranked_2 = (
    top_per_group
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_dense_rank",
        "minutes_since_prev_event_at",
        "event_at_tercile",
        "prev2_event_at",
        "event_at_cume_dist",
        "event_at_cume_dist_2",
        "last_event_at",
        "seq_no_2",
        "event_at_quartile",
        "first_event_at",
        "second_event_at",
        F.min("event_at_quartile").over(w_first_event_at_month).alias("first_event_at_month_min_event_at_quartile"),
        F.round(F.avg("minutes_since_prev_event_at").over(w_event_at_quartile_by_last_ev), 2).alias("minutes_since_prev_event_at_avg_2_prior"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at_quartile))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.lag("seq_no_2").over(w_event_at_quartile_by_last_ev_2).alias("prev_seq_no_2"),
    )
)

rollup_base = (
    ranked_2
    .filter(F.col("minutes_since_prev_event_at_2") > 2000.0)
    .groupBy(F.to_date("second_event_at").alias("second_event_at_day"))
    .agg(
        F.max("second_event_at").alias("last_second_event_at"),
        F.percentile_approx("first_event_at_month_min_event_at_quartile", 0.5).alias("median_first_event_at_month_min_event_at_quartil"),
    )
)

result = (
    rollup_base
    .select(
        "second_event_at_day",
        "last_second_event_at",
        "median_first_event_at_month_min_event_at_quartil",
        F.date_trunc("month", F.col("second_event_at_day")).alias("second_event_at_day_month"),
    )
    .orderBy(F.col("last_second_event_at").desc())
)
