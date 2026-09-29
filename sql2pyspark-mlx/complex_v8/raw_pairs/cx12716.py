from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_ticket_id_by = Window.partitionBy(F.date_trunc("month", "event_at"), "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id = Window.partitionBy("agent_id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_dedup = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("hours_since_open_vs_agent_id_avg").desc(), F.col("id").desc())

matched = (
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

metrics = (
    matched
    .select(
        "*",
        F.nth_value("hours_since_open", 2).over(w_event_at_month_ticket_id_by).alias("second_hours_since_open"),
        F.max("hours_since_open").over(w_agent_id).alias("agent_id_max_hours_since_open"),
        (F.col("hours_since_open") - F.lag("hours_since_open").over(w_all_by_event_at)).alias("hours_since_open_change"),
        F.round(F.col("hours_since_open") - F.avg("hours_since_open").over(w_agent_id), 2).alias("hours_since_open_vs_agent_id_avg"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "second_hours_since_open",
        "hours_since_open_change",
        "hours_since_open_vs_agent_id_avg",
        F.coalesce(F.col("hours_since_open_change"), F.lit(0.0)).alias("hours_since_open_change_filled"),
    )
)
