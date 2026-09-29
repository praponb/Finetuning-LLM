from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc())
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_ticket_id_by_event_at_3 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy("event_at")

valid_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 17)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

sequenced = (
    valid_events
    .select(
        "*",
        F.round(F.cume_dist().over(w_actor_ticket_id_by_event_at), 3).alias("event_at_cume_dist"),
        F.ntile(10).over(w_actor_ticket_id_by_event_at_2).alias("event_at_decile"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.rank().over(w_actor_by_event_at).alias("event_at_rank"),
        F.round(F.percent_rank().over(w_actor_by_event_at), 3).alias("event_at_percent_rank"),
        F.row_number().over(w_actor_by_event_at).alias("seq_no"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.percent_rank().over(w_actor_ticket_id_by_event_at), 3).alias("event_at_percent_rank_2"),
        F.nth_value("event_at", 2).over(w_actor_ticket_id_by_event_at_3).alias("second_event_at_2"),
        F.first("event_at").over(w_ticket_id_by_event_at_full).alias("first_event_at"),
        F.row_number().over(w_ticket_id_by_event_at).alias("rn"),
        F.first("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at"),
        F.lag("event_at", 3).over(w_event_type_by_event_at).alias("prev3_event_at"),
        F.round(F.cume_dist().over(w_event_type_by_event_at_ties), 3).alias("event_at_cume_dist_2"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at_cume_dist",
        "event_at_decile",
        "event_at_rank",
        "event_at_percent_rank",
        "seq_no",
        "minutes_since_prev_event_at",
        "second_event_at_2",
        "first_event_at",
        "first_known_event_at",
        "event_at_cume_dist_2",
        F.to_date(F.col("first_known_event_at")).alias("first_known_event_at_date"),
        F.round(F.col("event_at_cume_dist_2") * 1.5, 2).alias("event_at_cume_dist_2_adj"),
    )
    .orderBy("id")
    .limit(5)
)
