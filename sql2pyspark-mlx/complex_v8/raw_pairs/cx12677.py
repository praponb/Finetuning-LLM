from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_event_at_running = Window.partitionBy("priority").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_by_event_count_r = Window.partitionBy("event_count").orderBy("event_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
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

with_activity = (
    joined_ranges.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count"),
    )
)

summarized_rows = (
    with_activity.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

metrics = (
    summarized_rows
    .select(
        "*",
        F.last("event_count", ignorenulls=True).over(w_priority_by_event_at_running).alias("last_known_event_count"),
        F.last("event_count", ignorenulls=True).over(w_event_count_by_event_count_r).alias("last_known_event_count_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ticket_id",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "first_event_at",
        "event_count",
        "last_known_event_count",
        "last_known_event_count_2",
    )
    .orderBy(F.col("agent_id").asc_nulls_last(), "id")
    .limit(5)
)
