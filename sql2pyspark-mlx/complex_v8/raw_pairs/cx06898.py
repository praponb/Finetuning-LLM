from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_at_month_by = Window.partitionBy("ticket_id", F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at = Window.orderBy("event_at", "id")

scoped = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
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
        "*",
        F.round(F.cume_dist().over(w_ticket_id_event_at_month_by), 3).alias("event_at_cume_dist"),
        F.row_number().over(w_ticket_id_event_at_month_by).alias("rn"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

actor_stats = (
    with_windows
    .groupBy("actor")
    .agg(F.avg("minutes_since_prev_event_at").alias("avg_minutes_since_prev_event_at"))
)

above_average = (
    with_windows.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.minutes_since_prev_event_at") > F.col("g.avg_minutes_since_prev_event_at"))
    .select(
        "s.*",
        "g.avg_minutes_since_prev_event_at",
        F.round(F.col("s.minutes_since_prev_event_at") - F.col("g.avg_minutes_since_prev_event_at"), 2).alias("minutes_since_prev_event_at_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_cume_dist",
        "minutes_since_prev_event_at",
        "minutes_since_prev_event_at_minus_avg",
        F.year(F.col("event_at")).alias("event_at_year"),
    )
)
