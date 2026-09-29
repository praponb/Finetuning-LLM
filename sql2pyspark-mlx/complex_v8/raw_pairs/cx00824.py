from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at_week").desc(), "id")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_ties = Window.orderBy("event_at")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at_week", "id")
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at_week").desc())
w_event_at_month_by_event_at_r = Window.partitionBy(F.month("event_at")).orderBy(F.col("event_at_rank").desc(), "id")
w_event_at_week_dense_rank = Window.partitionBy("event_at_week_dense_rank")
w_rn_by_first_known_event_at = Window.partitionBy("rn").orderBy("first_known_event_at", "id")

base = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
)

metrics = (
    base
    .select(
        "id",
        "event_at",
        "actor",
        "actor_event_type_label",
        F.lead("event_at", 3).over(w_event_type_ticket_id_by_even).alias("next3_event_at"),
        F.first("event_at_week", ignorenulls=True).over(w_actor_ticket_id_by_event_at).alias("first_known_event_at_week"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.lead("event_at_week").over(w_all_by_event_at).alias("next_event_at_week"),
        F.lag("event_at_week", 3).over(w_all_by_event_at).alias("prev3_event_at_week"),
        F.row_number().over(w_event_type_ticket_id_by_even).alias("rn"),
        F.ntile(10).over(w_actor_ticket_id_by_event_at_2).alias("event_at_week_decile"),
        F.dense_rank().over(w_event_type_ticket_id_by_even_2).alias("event_at_week_dense_rank"),
        F.round((F.unix_timestamp("event_at_week") - F.unix_timestamp(F.lag("event_at_week").over(w_event_type_ticket_id_by_even))) / 60, 1).alias("minutes_since_prev_event_at_week"),
        F.lag("event_at_week").over(w_all_by_event_at).alias("prev_event_at_week"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.ntile(3).over(w_event_at_month_by_event_at_r).alias("event_at_rank_tercile"),
        F.min("event_at_week_decile").over(w_event_at_week_dense_rank).alias("event_at_week_dense_rank_min_event_at_week_decil"),
        F.round(F.cume_dist().over(w_rn_by_first_known_event_at), 3).alias("first_known_event_at_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "id",
        "event_at",
        "actor",
        "actor_event_type_label",
        "first_known_event_at_week",
        "event_at_rank",
        "event_at_cume_dist",
        "last_event_at",
        "next_event_at_week",
        "prev3_event_at_week",
        "rn",
        "event_at_week_decile",
        "event_at_week_dense_rank",
        "minutes_since_prev_event_at_week",
        "prev_event_at_week",
        "event_at_rank_tercile",
        "event_at_week_dense_rank_min_event_at_week_decil",
        "first_known_event_at_cume_dist",
    )
    .orderBy("event_at_week_dense_rank_min_event_at_week_decil")
)
