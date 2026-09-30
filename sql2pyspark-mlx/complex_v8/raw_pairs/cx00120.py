from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hours_since_open_ties = Window.orderBy("hours_since_open")
w_all_by_hours_since_open_runn = Window.orderBy("hours_since_open", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hours_since_open_full = Window.orderBy("hours_since_open", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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
        F.dense_rank().over(w_all_by_hours_since_open_ties).alias("hours_since_open_dense_rank"),
        F.first("event_at", ignorenulls=True).over(w_all_by_hours_since_open_runn).alias("first_known_event_at"),
        F.max("hours_since_open").over(w_all_by_hours_since_open_runn).alias("max_hours_since_open_so_far"),
        F.nth_value("event_at", 2).over(w_all_by_hours_since_open_full).alias("second_event_at"),
        F.nth_value("hours_since_open", 2).over(w_all_by_hours_since_open_full).alias("second_hours_since_open"),
    )
)

result = (
    sequenced
    .cube("agent_id", "hours_since_open_dense_rank", "event_type")
    .agg(
        F.grouping("agent_id").alias("is_total_agent_id"),
        F.max("event_at").alias("last_event_at"),
    )
)
