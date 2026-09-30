from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_priority = Window.partitionBy("event_type", "priority")
w_agent_id_by_event_at = Window.partitionBy("agent_id").orderBy(F.col("event_at").desc(), "id")
w_priority_event_type_by_event = Window.partitionBy("priority", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

joined_ranges = (
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

sequenced = (
    joined_ranges
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "hours_since_open",
        F.max("hours_since_open").over(w_event_type_priority).alias("event_type_priority_max_hours_since_open"),
        F.round(F.avg("hours_since_open").over(w_event_type_priority), 2).alias("event_type_priority_avg_hours_since_open"),
        F.round(F.cume_dist().over(w_agent_id_by_event_at), 3).alias("event_at_cume_dist"),
        F.nth_value("event_at", 2).over(w_priority_event_type_by_event).alias("second_event_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "hours_since_open",
        "event_type_priority_max_hours_since_open",
        "event_type_priority_avg_hours_since_open",
        "event_at_cume_dist",
        "second_event_at",
        F.round(F.col("hours_since_open") * 0.9, 2).alias("hours_since_open_adj"),
    )
    .orderBy(
        F.col("priority").desc(),
        F.col("event_at_cume_dist").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
