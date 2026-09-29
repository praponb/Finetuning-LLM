from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_event_at_year_by_event_at_fu = Window.partitionBy(F.year("event_at")).orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_event_at_year_by_event_at = Window.partitionBy(F.year("event_at")).orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_at_year_by_event_at_ru = Window.partitionBy(F.year("event_at")).orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_year_by_event_at_ti = Window.partitionBy(F.year("event_at")).orderBy(F.col("event_at").asc_nulls_last())
w_event_at_decile_2_event_at_y = Window.partitionBy("event_at_decile_2", F.year("event_at")).orderBy("first_known_event_at")
w_event_at_decile_2_event_at_y_2 = Window.partitionBy("event_at_decile_2", F.year("event_at")).orderBy("first_known_event_at", "id")
w_ticket_id_by_event_at_decile = Window.partitionBy("ticket_id").orderBy(F.col("event_at_decile_2").desc_nulls_first(), "id")
w_ticket_id = Window.partitionBy("ticket_id")

scoped_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp"))
)

ranked = (
    scoped_events
    .select(
        "*",
        F.dense_rank().over(w_event_type_ticket_id_by_even).alias("event_at_dense_rank"),
        F.lag("event_at", 2).over(w_event_type_ticket_id_by_even).alias("prev2_event_at"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.nth_value("event_at", 2).over(w_event_at_year_by_event_at_fu).alias("second_event_at"),
        F.ntile(10).over(w_event_type_ticket_id_by_even).alias("event_at_decile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lag("event_at").over(w_event_at_year_by_event_at).alias("prev_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_at_year_by_event_at_ru).alias("first_known_event_at"),
        F.round(F.percent_rank().over(w_event_at_year_by_event_at_ti), 3).alias("event_at_percent_rank"),
        F.ntile(10).over(w_actor_by_event_at).alias("event_at_decile_2"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_dense_rank",
        "prev2_event_at",
        "second_event_at",
        "minutes_since_prev_event_at",
        "prev_event_at",
        "first_known_event_at",
        "event_at_decile_2",
        F.rank().over(w_event_at_decile_2_event_at_y).alias("first_known_event_at_rank"),
        F.dense_rank().over(w_event_at_decile_2_event_at_y).alias("first_known_event_at_dense_rank"),
        F.row_number().over(w_event_at_decile_2_event_at_y_2).alias("first_known_event_at_seq"),
        F.row_number().over(w_ticket_id_by_event_at_decile).alias("seq_no_2"),
        F.round(F.avg("event_at_decile").over(w_ticket_id), 2).alias("ticket_id_avg_event_at_decile"),
        F.size(F.collect_set("actor").over(w_ticket_id)).alias("distinct_actor_in_ticket_id"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_dense_rank",
        "second_event_at",
        "prev_event_at",
        "first_known_event_at",
        "event_at_decile_2",
        "first_known_event_at_rank",
        "first_known_event_at_dense_rank",
        "first_known_event_at_seq",
        "seq_no_2",
        "ticket_id_avg_event_at_decile",
        "distinct_actor_in_ticket_id",
    )
    .orderBy(F.col("event_at").asc())
)
