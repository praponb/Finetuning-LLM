from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_event_type = Window.partitionBy(F.to_date("event_at"), "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_actor_by_even = Window.partitionBy(F.date_trunc("month", "event_at"), "actor").orderBy("event_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id")
w_actor_event_type_by_event_at_3 = Window.partitionBy("actor", "event_type").orderBy("event_at_week", "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_actor_event_type_by_event_at_4 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc())
w_event_at_to_date_event_type_2 = Window.partitionBy(F.to_date("event_at"), "event_type").orderBy("event_at", "id")
w_event_at_month_actor_by_even_2 = Window.partitionBy(F.date_trunc("month", "event_at"), "actor").orderBy("event_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_event_type_by_event_at_5 = Window.partitionBy("actor", "event_type").orderBy("event_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_actor_by_even_3 = Window.partitionBy(F.date_trunc("month", "event_at"), "actor").orderBy("event_at_week", "id")
w_second_event_at_week_2_month = Window.partitionBy(F.month("second_event_at_week_2"), "second_event_at_week_year").orderBy(F.col("event_at_seq").desc(), "id")
w_all_by_last_known_event_at_w = Window.orderBy("last_known_event_at_week", "id")
w_event_at_seq_by_second_event = Window.partitionBy("event_at_seq").orderBy("second_event_at_week_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_second_event_at_week = Window.orderBy(F.col("second_event_at_week_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_week_rank_second_ev = Window.partitionBy("event_at_week_rank", "second_event_at_week_year")

valid_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

with_windows = (
    valid_events
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_week",
        F.nth_value("event_at", 2).over(w_actor_event_type_by_event_at).alias("second_event_at"),
        F.last("event_at").over(w_event_at_to_date_event_type).alias("last_event_at"),
        F.nth_value("event_at_week", 2).over(w_event_at_month_actor_by_even).alias("second_event_at_week"),
        F.last("event_at_week").over(w_event_at_to_date_event_type).alias("last_event_at_week"),
        F.row_number().over(w_actor_event_type_by_event_at_2).alias("event_at_seq"),
        F.row_number().over(w_actor_event_type_by_event_at_3).alias("seq_no"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_event_type_by_even))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.percent_rank().over(w_actor_event_type_by_event_at_4), 3).alias("event_at_percent_rank"),
        F.dense_rank().over(w_event_at_to_date_event_type_2).alias("event_at_dense_rank"),
        F.last("event_at_week", ignorenulls=True).over(w_event_at_month_actor_by_even_2).alias("last_known_event_at_week"),
        F.last("event_at").over(w_actor_event_type_by_event_at).alias("last_event_at_2"),
        F.lead("event_at").over(w_ticket_id_event_type_by_even).alias("next_event_at"),
        F.round(F.percent_rank().over(w_actor_event_type_by_event_at_2), 3).alias("event_at_percent_rank_2"),
        F.nth_value("event_at_week", 2).over(w_actor_event_type_by_event_at_5).alias("second_event_at_week_2"),
        F.rank().over(w_event_at_month_actor_by_even_3).alias("event_at_week_rank"),
    )
)

tagged = (
    with_windows
    .filter(F.month(F.col("second_event_at_week")).isin(2, 3))
    .select(
        "id",
        "event_at_week",
        "second_event_at_week",
        "event_at_seq",
        "seq_no",
        "last_known_event_at_week",
        "event_at_percent_rank_2",
        "second_event_at_week_2",
        "event_at_week_rank",
        F.year(F.col("second_event_at_week")).alias("second_event_at_week_year"),
    )
)

sequenced = (
    tagged
    .select(
        "*",
        F.lag("last_known_event_at_week", 2).over(w_second_event_at_week_2_month).alias("prev2_last_known_event_at_week"),
        F.lead("second_event_at_week_year").over(w_all_by_last_known_event_at_w).alias("next_second_event_at_week_year"),
        F.last("last_known_event_at_week", ignorenulls=True).over(w_event_at_seq_by_second_event).alias("last_known_last_known_event_at_week"),
        F.last("event_at_week_rank").over(w_all_by_second_event_at_week).alias("last_event_at_week_rank"),
        F.row_number().over(w_all_by_last_known_event_at_w).alias("seq_no_2"),
        F.round(F.col("second_event_at_week_year") - F.avg("second_event_at_week_year").over(w_event_at_week_rank_second_ev), 2).alias("second_event_at_week_year_vs_event_at_week_rank_"),
        F.max("seq_no").over(w_event_at_week_rank_second_ev).alias("event_at_week_rank_second_event_at_week_year_max"),
        F.first("second_event_at_week_year", ignorenulls=True).over(w_event_at_seq_by_second_event).alias("first_known_second_event_at_week_year"),
    )
)

result = (
    sequenced
    .filter(F.col("seq_no_2") <= 9)
    .select(
        "id",
        "event_at_week",
        "seq_no",
        "last_known_event_at_week",
        "event_at_percent_rank_2",
        "second_event_at_week_2",
        "second_event_at_week_year",
        "prev2_last_known_event_at_week",
        "last_known_last_known_event_at_week",
        "last_event_at_week_rank",
        "seq_no_2",
        "event_at_week_rank_second_event_at_week_year_max",
        "first_known_second_event_at_week_year",
        F.round(F.col("event_at_percent_rank_2") * 1.07, 2).alias("event_at_percent_rank_2_adj"),
    )
    .orderBy("event_at_percent_rank_2", F.col("event_at_percent_rank_2_adj").desc())
)
