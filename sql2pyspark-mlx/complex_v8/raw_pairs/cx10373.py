from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at_full = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_ties = Window.partitionBy("ticket_id").orderBy("event_at")
w_event_type_event_at_month_by = Window.partitionBy("event_type", F.month("event_at")).orderBy("event_at")
w_actor_event_type_by_event_at_3 = Window.partitionBy("actor", "event_type").orderBy("event_at")
w_actor_by_event_at_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_type_by_event_at_4 = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_event_at_month_by_2 = Window.partitionBy("event_type", F.month("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_to_date_by_event_at_2 = Window.partitionBy(F.to_date("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.lead("event_at").over(w_actor_event_type_by_event_at).alias("next_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.nth_value("event_at", 2).over(w_actor_event_type_by_event_at_2).alias("second_event_at"),
        F.nth_value("event_at", 2).over(w_event_type_by_event_at_full).alias("second_event_at_2"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.round(F.percent_rank().over(w_event_type_event_at_month_by), 3).alias("event_at_percent_rank_2"),
        F.round(F.cume_dist().over(w_actor_event_type_by_event_at_3), 3).alias("event_at_cume_dist"),
        F.first("event_at", ignorenulls=True).over(w_actor_by_event_at_running).alias("first_known_event_at"),
        F.first("event_at").over(w_event_at_to_date_by_event_at).alias("first_event_at"),
        F.dense_rank().over(w_ticket_id_by_event_at_ties).alias("event_at_dense_rank"),
        F.first("event_at", ignorenulls=True).over(w_actor_event_type_by_event_at_4).alias("first_known_event_at_2"),
        F.first("event_at", ignorenulls=True).over(w_event_type_event_at_month_by_2).alias("first_known_event_at_3"),
        F.first("event_at", ignorenulls=True).over(w_event_at_to_date_by_event_at_2).alias("first_known_event_at_4"),
    )
)

labeled = (
    with_trends
    .select(
        "id",
        "event_at",
        "actor",
        "event_at_label",
        "next_event_at",
        "minutes_since_prev_event_at",
        "second_event_at_2",
        "event_at_percent_rank",
        "event_at_percent_rank_2",
        "event_at_cume_dist",
        "first_known_event_at",
        "first_event_at",
        "event_at_dense_rank",
        "first_known_event_at_3",
        "first_known_event_at_4",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

result = (
    labeled
    .select(
        "id",
        "actor",
        "event_at_label",
        "second_event_at_2",
        "event_at_percent_rank",
        "event_at_percent_rank_2",
        "event_at_cume_dist",
        "first_known_event_at_3",
        "days_since_event_at",
    )
    .orderBy("first_known_event_at_3", "id")
    .limit(5)
)
