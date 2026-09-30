from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_priority_actor_by_eve = Window.partitionBy("ticket_priority", "actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_priority_ticket_id_by = Window.partitionBy("ticket_priority", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_full_2 = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_priority_actor_by_eve_2 = Window.partitionBy("ticket_priority", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_full_3 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties_2 = Window.orderBy(F.col("event_at").asc_nulls_last())
w_next3_event_at_month_by_firs = Window.partitionBy(F.date_trunc("month", "next3_event_at")).orderBy("first_event_at")
w_second_event_at_year_prev_ev = Window.partitionBy(F.year("second_event_at"), F.year("prev_event_at_2"))
w_next3_event_at_month_by_firs_2 = Window.partitionBy(F.date_trunc("month", "next3_event_at")).orderBy("first_event_at", "id").rowsBetween(-3, -1)

filtered = (
    df_ticket_events
    .filter((F.col("event_type") != "replied") & (F.col("ticket_id") <= 4))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select("s.*", F.col("t.priority").alias("ticket_priority"))
)

windowed = (
    combined_details
    .select(
        "*",
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.lag("event_at").over(w_ticket_priority_actor_by_eve).alias("prev_event_at"),
        F.ntile(10).over(w_ticket_priority_ticket_id_by).alias("event_at_decile_2"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at_2"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full_2).alias("second_event_at"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.first("event_at").over(w_ticket_priority_actor_by_eve_2).alias("first_event_at_2"),
        F.last("event_at").over(w_all_by_event_at_full_3).alias("last_event_at"),
        F.lead("event_at", 3).over(w_ticket_priority_actor_by_eve).alias("next3_event_at"),
        F.first("event_at").over(w_all_by_event_at_full_3).alias("first_event_at_3"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties_2), 3).alias("event_at_cume_dist"),
    )
)

leaders = (
    windowed
    .filter(F.col("seq_no") == 1)
    .drop("seq_no")
)

with_trends = (
    leaders
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_priority",
        "event_at_decile",
        "event_at_dense_rank",
        "prev_event_at_2",
        "first_event_at",
        "second_event_at",
        "first_event_at_2",
        "last_event_at",
        "next3_event_at",
        "first_event_at_3",
        "event_at_cume_dist",
        F.dense_rank().over(w_next3_event_at_month_by_firs).alias("first_event_at_dense_rank"),
        F.round(F.col("event_at_cume_dist") - F.avg("event_at_cume_dist").over(w_second_event_at_year_prev_ev), 2).alias("event_at_cume_dist_vs_second_event_at_year_prev_"),
        F.max("event_at_decile").over(w_next3_event_at_month_by_firs_2).alias("event_at_decile_max_3_prior"),
        F.size(F.collect_set("ticket_priority").over(w_second_event_at_year_prev_ev)).alias("distinct_ticket_priority_in_second_event_at_year"),
    )
)

rollup_base = (
    with_trends
    .filter(F.to_date(F.col("first_event_at_3")) < F.lit("2025-01-31").cast("date"))
    .groupBy(F.year("first_event_at_3").alias("first_event_at_3_year"))
    .agg(
        F.min("first_event_at_2").alias("first_first_event_at_2"),
        F.percentile_approx("event_at_dense_rank", 0.5).alias("median_event_at_dense_rank"),
    )
)

result = (
    rollup_base
    .select(
        "first_event_at_3_year",
        "first_first_event_at_2",
        "median_event_at_dense_rank",
        F.date_trunc("hour", F.col("first_first_event_at_2")).alias("first_first_event_at_2_hour"),
    )
    .orderBy(
        F.col("median_event_at_dense_rank").desc(),
        F.col("first_event_at_3_year").asc_nulls_last(),
    )
)
