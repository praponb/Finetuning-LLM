from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_day = Window.partitionBy("ticket_id").orderBy("event_at_day", "id")
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at_day").desc(), "id")
w_ticket_id_by_event_at_day_fu = Window.partitionBy("ticket_id").orderBy("event_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_event_at_day_month_by_event = Window.partitionBy(F.month("event_at_day")).orderBy(F.col("event_at").desc_nulls_first())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at_day").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_day_month_by_event_2 = Window.partitionBy(F.month("event_at_day")).orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_second_event_at_day_2 = Window.orderBy(F.col("second_event_at_day_2").desc(), "id").rowsBetween(-3, -1)
w_all_by_minutes_since_prev_ev = Window.orderBy("minutes_since_prev_event_at_3", "id")
w_event_at_day_month = Window.partitionBy(F.month("event_at_day"))

cleaned = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(10, 16))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at_day))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.lead("event_at_day").over(w_ticket_id_actor_by_event_at).alias("next_event_at_day"),
        F.last("event_at_day").over(w_all_by_event_at_full).alias("last_event_at_day"),
        F.row_number().over(w_ticket_id_by_event_at_day).alias("rn"),
        F.round(F.cume_dist().over(w_ticket_id_by_event_at_day), 3).alias("event_at_day_cume_dist"),
        F.nth_value("event_at_day", 2).over(w_ticket_id_by_event_at_day_fu).alias("second_event_at_day"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.dense_rank().over(w_ticket_id_by_event_at_day).alias("event_at_day_dense_rank"),
        F.last("event_at").over(w_ticket_id_by_event_at_day_fu).alias("last_event_at_2"),
        F.rank().over(w_event_at_day_month_by_event).alias("event_at_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_3"),
        F.nth_value("event_at_day", 2).over(w_ticket_id_actor_by_event_at_2).alias("second_event_at_day_2"),
        F.lead("event_at_day", 3).over(w_event_at_day_month_by_event_2).alias("next3_event_at_day"),
        F.lead("event_at_day", 2).over(w_all_by_event_at).alias("next2_event_at_day"),
    )
)

top_per_group = (
    metrics
    .filter(F.col("rn") == 1)
    .drop("rn")
)

sequenced = (
    top_per_group
    .select(
        "*",
        F.max("event_at_dense_rank").over(w_all_by_second_event_at_day_2).alias("event_at_dense_rank_max_3_prior"),
        F.round(F.percent_rank().over(w_all_by_minutes_since_prev_ev), 3).alias("minutes_since_prev_event_at_3_percent_rank"),
        F.round(F.col("event_at_rank") - F.avg("event_at_rank").over(w_event_at_day_month), 2).alias("event_at_rank_vs_event_at_day_month_avg"),
        F.ntile(4).over(w_all_by_minutes_since_prev_ev).alias("minutes_since_prev_event_at_3_quartile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_day",
        "last_event_at",
        "minutes_since_prev_event_at",
        "next_event_at_day",
        "last_event_at_day",
        "event_at_day_cume_dist",
        "second_event_at_day",
        "event_at_dense_rank",
        "event_at_day_dense_rank",
        "last_event_at_2",
        "event_at_rank",
        "next2_event_at_day",
        "event_at_dense_rank_max_3_prior",
        "minutes_since_prev_event_at_3_percent_rank",
        "event_at_rank_vs_event_at_day_month_avg",
        "minutes_since_prev_event_at_3_quartile",
    )
    .orderBy(F.col("event_at").desc(), "id")
    .limit(5)
)
