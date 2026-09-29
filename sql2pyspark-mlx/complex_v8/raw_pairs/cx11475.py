from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy("event_at")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_event_at_to_date_by_event_at_2 = Window.partitionBy(F.to_date("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_event_at_2 = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_actor_ticket_id_by_event_at_3 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_dense_rank_event_at = Window.partitionBy("event_at_dense_rank", "event_at_seq")
w_all_by_first_known_event_at = Window.orderBy(F.col("first_known_event_at").asc_nulls_last(), "id")

valid_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 29)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    valid_events
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.dense_rank().over(w_ticket_id_by_event_at_ties).alias("event_at_dense_rank"),
        F.row_number().over(w_actor_by_event_at).alias("event_at_seq"),
        F.rank().over(w_event_at_to_date_by_event_at).alias("event_at_rank"),
        F.last("event_at", ignorenulls=True).over(w_actor_ticket_id_by_event_at).alias("last_known_event_at"),
        F.lag("event_at").over(w_ticket_id_by_event_at).alias("prev_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_at_to_date_by_event_at_2).alias("first_known_event_at"),
        F.lead("event_at").over(w_actor_by_event_at_2).alias("next_event_at"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at"),
        F.lag("event_at", 2).over(w_actor_by_event_at).alias("prev2_event_at"),
        F.ntile(10).over(w_actor_ticket_id_by_event_at_2).alias("event_at_decile"),
        F.nth_value("event_at", 2).over(w_actor_ticket_id_by_event_at_3).alias("second_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_ticket_id_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "event_type",
        "prev_event_at",
        "first_known_event_at",
        "next_event_at",
        "prev2_event_at",
        "second_event_at",
        "minutes_since_prev_event_at",
        F.round(F.col("event_at_seq") / F.nullif(F.sum("event_at_seq").over(w_event_at_dense_rank_event_at), F.lit(0)), 4).alias("event_at_seq_share_of_event_at_dense_rank_event_"),
        F.lag("prev_event_at").over(w_all_by_first_known_event_at).alias("prev_prev_event_at"),
        F.max("event_at_decile").over(w_event_at_dense_rank_event_at).alias("event_at_dense_rank_event_at_seq_max_event_at_de"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "event_type",
        "prev_event_at",
        "first_known_event_at",
        "next_event_at",
        "prev2_event_at",
        "second_event_at",
        "event_at_seq_share_of_event_at_dense_rank_event_",
        "prev_prev_event_at",
        "event_at_dense_rank_event_at_seq_max_event_at_de",
        F.year(F.col("next_event_at")).alias("next_event_at_year"),
        F.hour(F.col("next_event_at")).alias("next_event_at_hour"),
    )
    .orderBy("id", "prev2_event_at")
)
