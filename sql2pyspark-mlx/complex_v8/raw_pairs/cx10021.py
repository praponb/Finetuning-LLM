from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_actor_by_ticket_cl = Window.partitionBy("ticket_id", "actor").orderBy(F.col("ticket_closed_at").desc(), "id")
w_event_type_ticket_id_by_tick = Window.partitionBy("event_type", "ticket_id").orderBy("ticket_closed_at")
w_event_type_ticket_id_by_tick_2 = Window.partitionBy("event_type", "ticket_id").orderBy("ticket_closed_at", "id")
w_event_type_actor_by_ticket_c = Window.partitionBy("event_type", "actor").orderBy("ticket_closed_at", "id")
w_ticket_closed_at_month_by_ev = Window.partitionBy(F.date_trunc("month", "ticket_closed_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_tick_3 = Window.partitionBy("event_type", "ticket_id").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_actor_by_ticket_c_2 = Window.partitionBy("event_type", "actor").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_ticket_id_by_ticket_cl = Window.partitionBy("actor", "ticket_id").orderBy(F.col("ticket_closed_at").desc(), "id")
w_ticket_id_actor_by_ticket_cl_2 = Window.partitionBy("ticket_id", "actor").orderBy("ticket_closed_at")
w_ticket_closed_at_month_by_ev_2 = Window.partitionBy(F.date_trunc("month", "ticket_closed_at")).orderBy("event_at")
w_ticket_id_actor_by_ticket_cl_3 = Window.partitionBy("ticket_id", "actor").orderBy("ticket_closed_at", "id")
w_event_type_actor_by_ticket_c_3 = Window.partitionBy("event_type", "actor").orderBy("ticket_closed_at")
w_all_by_cnt_running = Window.orderBy("cnt", "ticket_closed_at_rank_2", "event_at_month", "ticket_closed_at_rank_3").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_closed_at_rank_2 = Window.partitionBy("ticket_closed_at_rank_2")

valid_events = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 6) & (F.hour(F.col("event_at")).between(7, 13)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

joined = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

ranked = (
    joined
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_ticket_id_actor_by_ticket_cl))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
        F.rank().over(w_event_type_ticket_id_by_tick).alias("ticket_closed_at_rank"),
        F.lag("ticket_closed_at", 3).over(w_event_type_ticket_id_by_tick_2).alias("prev3_ticket_closed_at"),
        F.lag("event_at").over(w_event_type_actor_by_ticket_c).alias("prev_event_at"),
        F.nth_value("event_at", 2).over(w_ticket_closed_at_month_by_ev).alias("second_event_at"),
        F.nth_value("ticket_closed_at", 2).over(w_event_type_ticket_id_by_tick_3).alias("second_ticket_closed_at"),
        F.nth_value("ticket_closed_at", 2).over(w_event_type_actor_by_ticket_c_2).alias("second_ticket_closed_at_2"),
        F.round(F.percent_rank().over(w_actor_ticket_id_by_ticket_cl), 3).alias("ticket_closed_at_percent_rank"),
        F.rank().over(w_ticket_id_actor_by_ticket_cl_2).alias("ticket_closed_at_rank_2"),
        F.round(F.cume_dist().over(w_ticket_closed_at_month_by_ev_2), 3).alias("event_at_cume_dist"),
        F.lag("ticket_closed_at").over(w_event_type_actor_by_ticket_c).alias("prev_ticket_closed_at"),
        F.lag("ticket_closed_at").over(w_ticket_id_actor_by_ticket_cl_3).alias("prev_ticket_closed_at_2"),
        F.rank().over(w_event_type_actor_by_ticket_c_3).alias("ticket_closed_at_rank_3"),
        F.round(F.percent_rank().over(w_ticket_id_actor_by_ticket_cl), 3).alias("ticket_closed_at_percent_rank_2"),
    )
)

per_group = (
    ranked
    .groupBy(
        "ticket_closed_at_rank_2",
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "ticket_closed_at_rank_3",
    )
    .agg(
        F.max("prev3_ticket_closed_at").alias("last_prev3_ticket_closed_at"),
        F.count("*").alias("cnt"),
    )
)

with_trends = (
    per_group
    .withColumn("min_cnt_so_far", F.min("cnt").over(w_all_by_cnt_running))
    .withColumn("ticket_closed_at_rank_2_sum_ticket_closed_at_ran", F.sum("ticket_closed_at_rank_2").over(w_ticket_closed_at_rank_2))
)

result = (
    with_trends
    .filter(F.col("event_at_month") < F.lit("2025-02-05 09:00:00").cast("timestamp"))
    .select(
        "ticket_closed_at_rank_2",
        "event_at_month",
        "ticket_closed_at_rank_3",
        "last_prev3_ticket_closed_at",
        "min_cnt_so_far",
        "ticket_closed_at_rank_2_sum_ticket_closed_at_ran",
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
        F.date_trunc("hour", F.col("event_at_month")).alias("event_at_month_hour"),
    )
    .orderBy(
        F.col("ticket_closed_at_rank_2").desc(),
        "event_at_month",
        "ticket_closed_at_rank_3",
    )
    .limit(20)
)
