from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc())
w_actor_event_type_by_event_at_2 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_event_type_by_event_at_3 = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_id = Window.partitionBy("ticket_id")

filtered = (
    df_ticket_events
    .filter((F.col("actor") == "agent") | (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

windowed = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_month",
        F.round(F.percent_rank().over(w_actor_event_type_by_event_at), 3).alias("event_at_percent_rank"),
        F.nth_value("event_at_month", 2).over(w_actor_event_type_by_event_at_2).alias("second_event_at_month"),
        (F.col("event_at_month") - F.lag("event_at_month").over(w_actor_event_type_by_event_at_3)).alias("event_at_month_change"),
        F.round(F.col("event_at_month") - F.avg("event_at_month").over(w_ticket_id), 2).alias("event_at_month_vs_ticket_id_avg"),
    )
)

event_type_stats = (
    windowed
    .groupBy("event_type")
    .agg(F.percentile_approx("event_at_percent_rank", 0.5).alias("median_event_at_percent_rank"))
)

above_average = (
    windowed.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.event_at_percent_rank") >= F.col("g.median_event_at_percent_rank"))
    .select(
        "s.*",
        "g.median_event_at_percent_rank",
        F.round(F.col("s.event_at_percent_rank") - F.col("g.median_event_at_percent_rank"), 2).alias("event_at_percent_rank_minus_median"),
    )
)

qualified = (
    above_average.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()) & (~F.col("t.event_type").isin("replied", "resolved")), "left_semi")
)

with_details = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.hour(F.col("t.closed_at")).between(6, 10)), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_at_month",
        "s.event_at_percent_rank",
        "s.event_at_month_change",
        "s.event_at_month_vs_ticket_id_avg",
        "s.event_at_percent_rank_minus_median",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

result = (
    with_details
    .filter(F.col("event_at_month_change") > -6)
    .select(
        "id",
        "ticket_id",
        "event_at_month",
        "event_at_percent_rank",
        "event_at_month_vs_ticket_id_avg",
        "event_at_percent_rank_minus_median",
        "ticket_opened_at",
        F.date_trunc("month", F.col("ticket_opened_at")).alias("ticket_opened_at_month"),
    )
    .orderBy("id", F.col("event_at_month").asc_nulls_last())
)
