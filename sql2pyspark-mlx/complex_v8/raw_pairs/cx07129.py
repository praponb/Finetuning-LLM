from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_full_2 = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_full_3 = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc_nulls_first())
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy("event_at", "id")
w_all_by_event_at_ties_2 = Window.orderBy(F.col("event_at").asc_nulls_last())
w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_ties_3 = Window.orderBy("event_at")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy("event_at")
w_all_by_event_at_cume_dist_2 = Window.orderBy(F.col("event_at_cume_dist_2").desc())
w_event_type = Window.partitionBy("event_type")
w_all_by_second_event_at_3 = Window.orderBy("second_event_at_3", "id")
w_last_event_at_year = Window.partitionBy(F.year("last_event_at"))
w_all_by_next2_event_at_moving = Window.orderBy(F.col("next2_event_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_ticket_id_by_second_event_at = Window.partitionBy("ticket_id").orderBy("second_event_at_3", "id")

scoped = (
    df_ticket_events
    .filter((F.col("actor") != "customer") & (F.month(F.col("event_at")).isin(1, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        F.lag("event_at").over(w_event_at_to_date_by_event_at).alias("prev_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full_2).alias("second_event_at_2"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full_3).alias("second_event_at_3"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.lead("event_at", 2).over(w_ticket_id_actor_by_event_at).alias("next2_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties_2), 3).alias("event_at_cume_dist"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties_3), 3).alias("event_at_cume_dist_2"),
        F.rank().over(w_ticket_id_actor_by_event_at_2).alias("event_at_rank"),
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("event_at_dense_rank") <= 3)
    .drop("event_at_dense_rank")
)

with_trends = (
    top_per_group
    .select(
        "*",
        F.dense_rank().over(w_all_by_event_at_cume_dist_2).alias("event_at_cume_dist_2_dense_rank"),
        F.round(F.col("event_at_rank") / F.nullif(F.sum("event_at_rank").over(w_event_type), F.lit(0)), 4).alias("event_at_rank_share_of_event_type"),
        F.lag("event_at").over(w_all_by_second_event_at_3).alias("prev_event_at_2"),
        F.round(F.col("event_at_cume_dist_2") - F.avg("event_at_cume_dist_2").over(w_event_type), 2).alias("event_at_cume_dist_2_vs_event_type_avg"),
        F.min("event_at_cume_dist_2").over(w_last_event_at_year).alias("last_event_at_year_min_event_at_cume_dist_2"),
        F.round(F.avg("event_at_cume_dist").over(w_last_event_at_year), 2).alias("last_event_at_year_avg_event_at_cume_dist"),
        F.round(F.avg("event_at_cume_dist_2").over(w_all_by_next2_event_at_moving), 2).alias("event_at_cume_dist_2_avg_2"),
        F.lag("event_at_cume_dist", 3).over(w_ticket_id_by_second_event_at).alias("prev3_event_at_cume_dist"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "second_event_at",
        "second_event_at_2",
        "first_event_at",
        "next2_event_at",
        "last_known_event_at",
        "event_at_cume_dist_2",
        "event_at_rank",
        "event_at_cume_dist_2_dense_rank",
        "event_at_rank_share_of_event_type",
        "prev_event_at_2",
        "last_event_at_year_avg_event_at_cume_dist",
        "event_at_cume_dist_2_avg_2",
        "prev3_event_at_cume_dist",
    )
    .orderBy(F.col("second_event_at_2").desc())
)
