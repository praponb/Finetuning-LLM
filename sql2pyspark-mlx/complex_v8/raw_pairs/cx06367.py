from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_ticket_id_by_event = Window.partitionBy("agent_id", "ticket_id").orderBy("event_at", "id")
w_event_at_month_priority_by_h = Window.partitionBy(F.date_trunc("month", "event_at"), "priority").orderBy("hours_since_open")
w_agent_id = Window.partitionBy("agent_id")
w_event_at_month_priority_by_h_2 = Window.partitionBy(F.date_trunc("month", "event_at"), "priority").orderBy("hours_since_open", "id")

in_window = (
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

windowed = (
    in_window
    .select(
        "*",
        F.lead("event_at").over(w_agent_id_ticket_id_by_event).alias("next_event_at"),
        F.round(F.cume_dist().over(w_event_at_month_priority_by_h), 3).alias("hours_since_open_cume_dist"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.ntile(4).over(w_event_at_month_priority_by_h_2).alias("hours_since_open_quartile"),
    )
)

per_group = (
    windowed
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type").isin("escalated", "created"), F.col("hours_since_open_cume_dist"))).alias("hours_since_open_cume_dist_event_type_in_escalat"),
        F.sum("hours_since_open_quartile").alias("total_hours_since_open_quartile"),
    )
    .filter(F.col("total_hours_since_open_quartile") > 2)
)

result = (
    per_group
    .filter(F.col("hours_since_open_cume_dist_event_type_in_escalat") >= 2.2)
    .select(
        "event_type",
        "hours_since_open_cume_dist_event_type_in_escalat",
        "total_hours_since_open_quartile",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
    .orderBy("event_type_clean")
)
