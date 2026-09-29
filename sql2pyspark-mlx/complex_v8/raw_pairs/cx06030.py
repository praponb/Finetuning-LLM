from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_event_type_or_unkno = Window.partitionBy("agent_id", "event_type_or_unknown").orderBy("event_at", "id")
w_event_at_to_date_by_event_at = Window.partitionBy(F.to_date("event_at")).orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

in_window = (
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

tagged = (
    in_window
    .select(
        "*",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

with_windows = (
    tagged
    .select(
        "id",
        "event_at",
        "event_type",
        "hours_since_open",
        "event_type_or_unknown",
        (F.col("hours_since_open") - F.lag("hours_since_open").over(w_agent_id_event_type_or_unkno)).alias("hours_since_open_change"),
        F.first("hours_since_open").over(w_event_at_to_date_by_event_at).alias("first_hours_since_open"),
    )
)

filtered = (
    with_windows
    .crossJoin(with_windows.agg(F.max("first_hours_since_open").alias("overall_max_first_hours_since_open")))
    .filter(F.col("first_hours_since_open") < F.col("overall_max_first_hours_since_open"))
    .drop("overall_max_first_hours_since_open")
)

result = (
    filtered
    .groupBy("event_type")
    .pivot("event_type_or_unknown", ["assigned", "UNKNOWN"])
    .agg(F.avg("hours_since_open"))
)
