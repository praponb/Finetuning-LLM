from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last())
w_all_by_ticket_closed_at_full = Window.orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_by_event_at = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_ticket_closed_at = Window.orderBy("ticket_closed_at", "id")
w_event_at_month_by_event_at_f = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_event_at_month_by_event = Window.partitionBy(F.month("last_event_at")).orderBy(F.col("event_at_rank").desc())
w_event_at_tercile_event_at_ra = Window.partitionBy("event_at_tercile", "event_at_rank").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_closed_at_quartile = Window.partitionBy("ticket_closed_at_quartile")
w_last_event_at_month_by_event_2 = Window.partitionBy(F.month("last_event_at")).orderBy(F.col("event_at_rank").desc(), "id")

valid_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

enriched_ticket = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_ref",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

ranked = (
    enriched_ticket
    .select(
        "*",
        F.round(F.percent_rank().over(w_actor_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.last("event_at").over(w_all_by_ticket_closed_at_full).alias("last_event_at"),
        F.lag("event_at").over(w_event_at_month_by_event_at).alias("prev_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_month_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.nth_value("ticket_closed_at", 2).over(w_all_by_ticket_closed_at_full).alias("second_ticket_closed_at"),
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
        F.ntile(4).over(w_all_by_ticket_closed_at).alias("ticket_closed_at_quartile"),
        F.last("event_at").over(w_event_at_month_by_event_at_f).alias("last_event_at_2"),
        F.last("event_at").over(w_actor_by_event_at_full).alias("last_event_at_3"),
        F.ntile(3).over(w_actor_by_event_at).alias("event_at_tercile"),
        F.rank().over(w_actor_by_event_at).alias("event_at_rank"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.round(F.percent_rank().over(w_last_event_at_month_by_event), 3).alias("event_at_rank_percent_rank"),
        F.first("last_event_at_3", ignorenulls=True).over(w_event_at_tercile_event_at_ra).alias("first_known_last_event_at_3"),
        F.round(F.avg("event_at_percent_rank").over(w_ticket_closed_at_quartile), 2).alias("ticket_closed_at_quartile_avg_event_at_percent_r"),
        F.sum("event_at_tercile").over(w_ticket_closed_at_quartile).alias("ticket_closed_at_quartile_sum_event_at_tercile"),
        F.count(F.lit(1)).over(w_ticket_closed_at_quartile).alias("rows_in_ticket_closed_at_quartile"),
        F.round(F.col("event_at_percent_rank") / F.nullif(F.sum("event_at_percent_rank").over(w_ticket_closed_at_quartile), F.lit(0)), 4).alias("event_at_percent_rank_share_of_ticket_closed_at_"),
        F.round(F.cume_dist().over(w_last_event_at_month_by_event_2), 3).alias("event_at_rank_cume_dist"),
        F.min("event_at_rank").over(w_ticket_closed_at_quartile).alias("ticket_closed_at_quartile_min_event_at_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_ref",
        "event_at_percent_rank",
        "last_event_at",
        "prev_event_at",
        "minutes_since_prev_event_at",
        "second_ticket_closed_at",
        "minutes_since_prev_ticket_closed_at",
        "ticket_closed_at_quartile",
        "last_event_at_2",
        "event_at_tercile",
        "event_at_rank",
        "ticket_closed_at_quartile_avg_event_at_percent_r",
        "ticket_closed_at_quartile_sum_event_at_tercile",
        "rows_in_ticket_closed_at_quartile",
        "event_at_percent_rank_share_of_ticket_closed_at_",
        "event_at_rank_cume_dist",
        F.coalesce(F.col("event_at_percent_rank"), F.lit(0.0)).alias("event_at_percent_rank_filled"),
        F.round(F.col("event_at_rank_cume_dist") * 1.5, 2).alias("event_at_rank_cume_dist_adj"),
    )
    .orderBy(
        F.col("event_at_tercile").desc(),
        F.col("event_at_rank_cume_dist_adj").desc(),
        "id",
    )
    .limit(10)
)
