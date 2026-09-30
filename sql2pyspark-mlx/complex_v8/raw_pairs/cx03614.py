from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc())
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_all_by_event_at_2 = Window.orderBy("event_at", "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc_nulls_first())
w_all_by_event_at_full_2 = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id")

prepared_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

windowed = (
    prepared_events
    .select(
        "id",
        "event_type",
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.round(F.cume_dist().over(w_ticket_id_event_type_by_even), 3).alias("event_at_cume_dist"),
        F.round(F.percent_rank().over(w_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.row_number().over(w_all_by_event_at_2).alias("event_at_seq"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank_2"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full_2).alias("second_event_at_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_event_type_by_even_2))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(4).over(w_actor_by_event_at).alias("event_at_quartile"),
        F.row_number().over(w_all_by_event_at).alias("rn"),
    )
)

top_rows = (
    windowed
    .filter((F.col("event_at_seq") <= 5) & (F.col("event_at_dense_rank") < 9))
    .drop("event_at_seq")
)

result = (
    top_rows
    .select(
        "id",
        "event_type",
        "event_at_dense_rank",
        "event_at_cume_dist",
        "event_at_percent_rank",
        "event_at_dense_rank_2",
        "minutes_since_prev_event_at",
        "event_at_quartile",
    )
    .orderBy(F.col("event_at_dense_rank").desc(), F.col("id").asc())
)
