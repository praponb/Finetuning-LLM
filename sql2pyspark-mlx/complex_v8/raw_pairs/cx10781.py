from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").desc_nulls_first())
w_priority_by_hours_since_open = Window.partitionBy("priority").orderBy("hours_since_open", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

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

with_windows = (
    joined_ranges
    .select(
        "*",
        F.round(F.percent_rank().over(w_event_type_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.first("event_at").over(w_priority_by_hours_since_open).alias("first_event_at"),
    )
)

exploded_detail_events = (
    with_windows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.agent_id",
        "s.hours_since_open",
        "s.event_at_percent_rank",
        "s.first_event_at",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

enriched = (
    exploded_detail_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .filter(F.col("t.ticket_id") <= 30)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.agent_id",
        "s.hours_since_open",
        "s.event_at_percent_rank",
        "s.first_event_at",
        "s.event_id",
        "s.event_actor",
        "s.event_event_type",
        "s.event_event_at",
        F.col("t.ticket_id").alias("event_ticket_id"),
        "t.event_at",
    )
)

result = (
    enriched
    .select(
        "id",
        "event_id",
        "event_at_percent_rank",
        "hours_since_open",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["event_at_percent_rank", "hours_since_open"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
