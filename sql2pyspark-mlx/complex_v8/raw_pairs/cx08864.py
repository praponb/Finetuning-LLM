from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_agen = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_ticket_csat_run = Window.partitionBy("ticket_id").orderBy("ticket_csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_ticket_csat = Window.partitionBy("ticket_id").orderBy("ticket_csat", "id")
w_ticket_id_by_event_at_moving = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_ticket_id_event_type = Window.partitionBy("ticket_id", "event_type")
w_event_type_by_agent_hired_on = Window.partitionBy("event_type").orderBy("agent_hired_on", "id")
w_event_type_by_agent_hired_on_2 = Window.partitionBy("event_type").orderBy("agent_hired_on", "id").rowsBetween(-2, -1)
w_all_by_event_at_full = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_running = Window.orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_events = (
    df_ticket_events
    .filter(F.col("actor").isin("system", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched_ticket = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.*",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

with_trends = (
    enriched_ticket
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "ticket_agent_id",
        "ticket_csat",
        "agent_hired_on",
        "agent_name",
        F.nth_value("event_at", 2).over(w_event_type_ticket_id_by_agen).alias("second_event_at"),
        F.max("ticket_csat").over(w_ticket_id_by_ticket_csat_run).alias("max_ticket_csat_so_far"),
        F.round(F.cume_dist().over(w_ticket_id_by_ticket_csat), 3).alias("ticket_csat_cume_dist"),
        F.round(F.avg("ticket_csat").over(w_ticket_id_by_event_at_moving), 2).alias("ticket_csat_avg_2"),
        F.count(F.lit(1)).over(w_ticket_id_event_type).alias("rows_in_ticket_id_event_type"),
        F.round(F.avg("ticket_csat").over(w_ticket_id_event_type), 2).alias("ticket_id_event_type_avg_ticket_csat"),
        F.round(F.cume_dist().over(w_event_type_by_agent_hired_on), 3).alias("agent_hired_on_cume_dist"),
        F.round(F.avg("ticket_csat").over(w_event_type_by_agent_hired_on_2), 2).alias("ticket_csat_avg_2_prior"),
        F.first("agent_hired_on").over(w_all_by_event_at_full).alias("first_agent_hired_on"),
        F.last("agent_hired_on", ignorenulls=True).over(w_all_by_event_at_running).alias("last_known_agent_hired_on"),
        F.max("ticket_csat").over(w_ticket_id_event_type).alias("ticket_id_event_type_max_ticket_csat"),
    )
)

result = (
    with_trends
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "ticket_id",
        "agent_name",
        "max_ticket_csat_so_far",
        "ticket_csat_cume_dist",
        "ticket_csat_avg_2",
        "rows_in_ticket_id_event_type",
        "ticket_id_event_type_avg_ticket_csat",
        "agent_hired_on_cume_dist",
        "ticket_csat_avg_2_prior",
        "first_agent_hired_on",
    )
    .orderBy("agent_hired_on_cume_dist")
)
