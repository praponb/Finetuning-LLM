from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_hours_since_open = Window.partitionBy("priority").orderBy(F.col("hours_since_open").desc())
w_priority_by_hours_since_open_2 = Window.partitionBy("priority").orderBy(F.col("hours_since_open").desc(), "id")

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
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

sequenced = (
    joined_ranges
    .select(
        "*",
        F.round(F.percent_rank().over(w_priority_by_hours_since_open), 3).alias("hours_since_open_percent_rank"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_priority_by_hours_since_open_2))) / 60, 1).alias("minutes_since_prev_event_at"),
    )
)

with_details = (
    sequenced.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        "s.hours_since_open_percent_rank",
        "s.minutes_since_prev_event_at",
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_details
    .select(
        "id",
        "ticket_id",
        "priority",
        "agent_id",
        "hours_since_open",
        "minutes_since_prev_event_at",
        "agent_team",
        F.abs(F.col("hours_since_open") - 28.8).alias("hours_since_open_distance"),
    )
    .orderBy(F.col("id").desc(), F.col("priority").desc())
    .limit(20)
)
