from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_type_by_event_at_3 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_event_at_ties = Window.orderBy("last_event_at")
w_all_by_last_event_at_moving = Window.orderBy("last_event_at", "id").rowsBetween(-3, Window.currentRow)
w_actor_ticket_id = Window.partitionBy("actor", "ticket_id")

recent_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

with_windows = (
    recent_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_ref",
        "event_type_prefix",
        F.lag("event_at").over(w_actor_event_type_by_event_at).alias("prev_event_at"),
        F.ntile(10).over(w_actor_event_type_by_event_at).alias("event_at_decile"),
        F.nth_value("event_at", 2).over(w_actor_event_type_by_event_at_2).alias("second_event_at"),
        F.first("event_at", ignorenulls=True).over(w_actor_event_type_by_event_at_3).alias("first_known_event_at"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at_2"),
        F.lead("event_at").over(w_actor_event_type_by_event_at).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_ref",
        "event_at_decile",
        "first_known_event_at",
        "first_known_event_at_2",
        "next_event_at",
        "minutes_since_prev_event_at",
        F.rank().over(w_all_by_last_event_at_ties).alias("last_event_at_rank"),
        F.round(F.avg("minutes_since_prev_event_at").over(w_all_by_last_event_at_moving), 2).alias("minutes_since_prev_event_at_avg_4"),
        F.round(F.col("event_at_decile") - F.avg("event_at_decile").over(w_actor_ticket_id), 2).alias("event_at_decile_vs_actor_ticket_id_avg"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "event_type",
        "event_type_ref",
        "first_known_event_at",
        "first_known_event_at_2",
        "next_event_at",
        "minutes_since_prev_event_at",
        "last_event_at_rank",
        "event_at_decile_vs_actor_ticket_id_avg",
        F.when(F.col("last_event_at_rank") >= 7, "large").when(F.col("last_event_at_rank") >= 4, "medium").otherwise("small").alias("last_event_at_rank_tier"),
    )
)
