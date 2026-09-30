from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hours_since_open_ties = Window.orderBy("hours_since_open")
w_event_type_ticket_id = Window.partitionBy("event_type", "ticket_id")
w_all_by_hours_since_open = Window.orderBy("hours_since_open", "id")
w_all_by_hours_since_open_runn = Window.orderBy("hours_since_open", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

ranked = (
    in_window
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_hours_since_open_ties), 3).alias("hours_since_open_percent_rank"),
        F.sum("hours_since_open").over(w_event_type_ticket_id).alias("event_type_ticket_id_sum_hours_since_open"),
        F.lag("hours_since_open", 3).over(w_all_by_hours_since_open).alias("prev3_hours_since_open"),
        F.first("event_at", ignorenulls=True).over(w_all_by_hours_since_open_runn).alias("first_known_event_at"),
        (F.col("hours_since_open") - F.lag("hours_since_open").over(w_all_by_hours_since_open)).alias("hours_since_open_change"),
        F.size(F.collect_set("agent_id").over(w_event_type_ticket_id)).alias("distinct_agent_id_in_event_type_ticket_id"),
    )
)

result = (
    ranked
    .select(
        "id",
        "event_type",
        "agent_id",
        "hours_since_open",
        "event_type_ticket_id_sum_hours_since_open",
        "prev3_hours_since_open",
        "first_known_event_at",
        "distinct_agent_id_in_event_type_ticket_id",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.round(F.col("prev3_hours_since_open") * 1.1, 2).alias("prev3_hours_since_open_adj"),
    )
    .orderBy(F.col("days_since_event_at").desc_nulls_first())
)
