from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_ticket_id = Window.partitionBy("priority", "ticket_id")
w_event_type_ticket_id_by_hour = Window.partitionBy("event_type", "ticket_id").orderBy("hours_since_open", "id")

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

windowed = (
    in_window
    .select(
        "*",
        F.count(F.lit(1)).over(w_priority_ticket_id).alias("rows_in_priority_ticket_id"),
        F.round(F.cume_dist().over(w_event_type_ticket_id_by_hour), 3).alias("hours_since_open_cume_dist"),
        (F.col("hours_since_open") - F.lag("hours_since_open").over(w_event_type_ticket_id_by_hour)).alias("hours_since_open_change"),
    )
)

result = (
    windowed
    .filter(~F.col("priority").isin("P2", "P4"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "hours_since_open",
        "hours_since_open_change",
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
    .orderBy("priority_ref", F.col("ticket_id").asc(), "id")
    .limit(5)
)
