from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_event_at_year_month = Window.partitionBy("priority", F.date_trunc("month", "event_at_year")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_to_date_priority = Window.partitionBy(F.to_date("event_at"), "priority")
w_priority_event_at_year_month_2 = Window.partitionBy("priority", F.date_trunc("month", "event_at_year")).orderBy("event_at")

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

derived = (
    covered
    .filter(F.col("agent_id") <= 6)
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "hours_since_open",
        F.coalesce(F.col("hours_since_open"), F.lit(0.0)).alias("hours_since_open_filled"),
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
)

ranked = (
    derived
    .select(
        "*",
        F.last("hours_since_open_filled").over(w_priority_event_at_year_month).alias("last_hours_since_open_filled"),
        F.sum("hours_since_open").over(w_event_at_to_date_priority).alias("event_at_to_date_priority_sum_hours_since_open"),
        F.round(F.percent_rank().over(w_priority_event_at_year_month_2), 3).alias("event_at_percent_rank"),
        F.round(F.col("hours_since_open_filled") - F.avg("hours_since_open_filled").over(w_event_at_to_date_priority), 2).alias("hours_since_open_filled_vs_event_at_to_date_prio"),
        F.round(F.avg("hours_since_open").over(w_event_at_to_date_priority), 2).alias("event_at_to_date_priority_avg_hours_since_open"),
    )
)

result = (
    ranked
    .select(
        "id",
        "event_at",
        "event_type",
        "priority",
        "hours_since_open_filled",
        "event_at_year",
        "event_at_to_date_priority_sum_hours_since_open",
        "event_at_percent_rank",
        "hours_since_open_filled_vs_event_at_to_date_prio",
        "event_at_to_date_priority_avg_hours_since_open",
    )
    .orderBy(F.col("event_at").desc())
)
