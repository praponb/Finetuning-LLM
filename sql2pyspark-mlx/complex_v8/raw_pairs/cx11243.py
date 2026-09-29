from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_closed_at_month_ticke = Window.partitionBy(F.date_trunc("month", "ticket_closed_at"), "ticket_id").orderBy("ticket_closed_at")
w_ticket_closed_at_month_ticke_2 = Window.partitionBy(F.date_trunc("month", "ticket_closed_at"), "ticket_id").orderBy("ticket_closed_at", "id")
w_ticket_closed_at_month_ticke_3 = Window.partitionBy(F.date_trunc("month", "ticket_closed_at"), "ticket_id").orderBy("ticket_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

joined_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

without_matches = (
    joined_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") < F.lit("2025-03-02 10:00:00").cast("timestamp")), "left_anti")
)

windowed = (
    without_matches
    .select(
        "*",
        F.rank().over(w_ticket_closed_at_month_ticke).alias("ticket_closed_at_rank"),
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_ticket_closed_at_month_ticke_2))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
        F.first("ticket_closed_at").over(w_ticket_closed_at_month_ticke_3).alias("first_ticket_closed_at"),
    )
)

event_type_stats = (
    windowed
    .groupBy("event_type")
    .agg(F.percentile_approx("ticket_closed_at_rank", 0.5).alias("median_ticket_closed_at_rank"))
)

vs_group = (
    windowed.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.ticket_closed_at_rank") >= F.col("g.median_ticket_closed_at_rank"))
    .select(
        "s.*",
        "g.median_ticket_closed_at_rank",
        F.round(F.col("s.ticket_closed_at_rank") - F.col("g.median_ticket_closed_at_rank"), 2).alias("ticket_closed_at_rank_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "ticket_id",
        "ticket_priority",
        "ticket_closed_at_rank",
        "minutes_since_prev_ticket_closed_at",
        "first_ticket_closed_at",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.abs(F.col("ticket_closed_at_rank") - 4).alias("ticket_closed_at_rank_distance"),
    )
    .orderBy(F.col("ticket_id").desc())
)
