from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties = Window.orderBy("event_at")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id")

prepared_events = df_ticket_events

sequenced = (
    prepared_events
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.lead("event_at").over(w_event_type_by_event_at).alias("next_event_at"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.round(F.cume_dist().over(w_event_type_by_event_at), 3).alias("event_at_cume_dist_2"),
        F.ntile(3).over(w_ticket_id_by_event_at).alias("event_at_tercile"),
    )
)

leaders = (
    sequenced
    .filter((F.col("event_at_dense_rank") <= 2) & (F.col("event_at_percent_rank") >= 0.3))
)

result = (
    leaders
    .select(
        "id",
        "event_type",
        "actor",
        "event_at_cume_dist",
        "last_known_event_at",
        "minutes_since_prev_event_at",
        "event_at_quintile",
        "second_event_at",
        "event_at_percent_rank",
        "next_event_at",
        "event_at_dense_rank",
        "event_at_cume_dist_2",
        "event_at_tercile",
        F.hour(F.col("second_event_at")).alias("second_event_at_hour"),
    )
    .orderBy(F.col("event_at_quintile").desc_nulls_first())
)
