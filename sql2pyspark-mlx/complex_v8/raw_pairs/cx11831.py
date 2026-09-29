from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_by_event_at_2 = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc())
w_event_type_event_at_tercile = Window.partitionBy("event_type", "event_at_tercile")
w_all_by_first_event_at = Window.orderBy(F.col("first_event_at").desc(), "id")
w_all_by_event_at_percent_rank = Window.orderBy(F.col("event_at_percent_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    prepared_events
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        F.round(F.percent_rank().over(w_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.ntile(3).over(w_ticket_id_event_type_by_even).alias("event_at_tercile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_event_type_by_even))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.first("event_at").over(w_ticket_id_event_type_by_even_2).alias("first_event_at"),
        F.lag("event_at", 2).over(w_ticket_id_event_type_by_even).alias("prev2_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_id_event_type_by_even_2).alias("second_event_at"),
        F.row_number().over(w_ticket_id_by_event_at).alias("seq_no"),
        F.ntile(10).over(w_ticket_id_by_event_at_2).alias("event_at_decile"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at_2"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.row_number().over(w_ticket_id_by_event_at_2).alias("rn"),
        F.lag("event_at").over(w_ticket_id_event_type_by_even).alias("prev_event_at"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_tercile",
        "minutes_since_prev_event_at",
        "first_event_at",
        "second_event_at",
        "seq_no",
        "event_at_decile",
        "first_event_at_2",
        "minutes_since_prev_event_at_2",
        "rn",
        "prev_event_at",
        F.round(F.col("event_at_percent_rank") / F.nullif(F.sum("event_at_percent_rank").over(w_event_type_event_at_tercile), F.lit(0)), 4).alias("event_at_percent_rank_share_of_event_type_event_"),
        F.round(F.cume_dist().over(w_all_by_first_event_at), 3).alias("first_event_at_cume_dist"),
        F.round(F.col("event_at_percent_rank") - F.avg("event_at_percent_rank").over(w_event_type_event_at_tercile), 2).alias("event_at_percent_rank_vs_event_type_event_at_ter"),
        F.size(F.collect_set("actor").over(w_event_type_event_at_tercile)).alias("distinct_actor_in_event_type_event_at_tercile"),
        F.nth_value("event_at_percent_rank", 2).over(w_all_by_event_at_percent_rank).alias("second_event_at_percent_rank"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "minutes_since_prev_event_at",
        "second_event_at",
        "seq_no",
        "event_at_decile",
        "first_event_at_2",
        "minutes_since_prev_event_at_2",
        "rn",
        "prev_event_at",
        "event_at_percent_rank_share_of_event_type_event_",
        "event_at_percent_rank_vs_event_type_event_at_ter",
        "distinct_actor_in_event_type_event_at_tercile",
    )
    .orderBy(F.col("event_type").desc())
)
