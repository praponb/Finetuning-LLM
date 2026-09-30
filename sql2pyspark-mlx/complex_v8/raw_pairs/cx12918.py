from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc())
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_quintile_ful = Window.orderBy("event_at_quintile", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_quintile_tie = Window.orderBy("event_at_quintile")
w_event_at_rank = Window.partitionBy("event_at_rank")
w_event_at_year_by_event_at_se = Window.partitionBy(F.year("event_at")).orderBy("event_at_seq")
w_next2_event_at_to_date_by_ne = Window.partitionBy(F.to_date("next2_event_at")).orderBy("next2_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_seq_next2_event_at = Window.partitionBy("event_at_seq", F.year("next2_event_at"))
w_all_by_event_at_quintile = Window.orderBy("event_at_quintile", "id")

prepared = (
    df_ticket_events
    .filter((F.col("event_type") == "assigned") & (F.col("event_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "event_at",
        "actor",
        F.lead("event_at", 2).over(w_ticket_id_actor_by_event_at).alias("next2_event_at"),
        F.row_number().over(w_ticket_id_event_type_by_even).alias("event_at_seq"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.ntile(5).over(w_ticket_id_event_type_by_even).alias("event_at_quintile"),
        F.round(F.percent_rank().over(w_ticket_id_event_type_by_even_2), 3).alias("event_at_percent_rank"),
        F.round(F.cume_dist().over(w_ticket_id_event_type_by_even), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_event_type_by_even))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_id_actor_by_event_at_2).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_3"),
    )
)

windowed = (
    with_windows
    .select(
        "id",
        "event_at",
        "next2_event_at",
        "event_at_seq",
        "event_at_rank",
        "event_at_percent_rank",
        "minutes_since_prev_event_at",
        F.last("event_at_percent_rank").over(w_all_by_event_at_quintile_ful).alias("last_event_at_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at_quintile_tie), 3).alias("event_at_quintile_cume_dist"),
        F.size(F.collect_set("actor").over(w_event_at_rank)).alias("distinct_actor_in_event_at_rank"),
        F.dense_rank().over(w_event_at_year_by_event_at_se).alias("event_at_seq_dense_rank"),
        F.first("event_at_rank", ignorenulls=True).over(w_next2_event_at_to_date_by_ne).alias("first_known_event_at_rank"),
        F.min("event_at_quintile").over(w_event_at_seq_next2_event_at).alias("event_at_seq_next2_event_at_year_min_event_at_qu"),
        F.row_number().over(w_all_by_event_at_quintile).alias("seq_no"),
    )
)

result = (
    windowed
    .select(
        "id",
        "event_at",
        "next2_event_at",
        "event_at_rank",
        "event_at_percent_rank",
        "minutes_since_prev_event_at",
        "last_event_at_percent_rank",
        "distinct_actor_in_event_at_rank",
        "event_at_seq_dense_rank",
        "first_known_event_at_rank",
        "event_at_seq_next2_event_at_year_min_event_at_qu",
        "seq_no",
        F.greatest(F.col("event_at_rank"), F.col("event_at_percent_rank")).alias("greatest_event_at_rank_event_at_percent_rank"),
        F.month(F.col("event_at")).alias("event_at_month"),
    )
    .orderBy(F.col("event_at_rank").desc())
)
