from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_csat_by_requester_id = Window.partitionBy("csat").orderBy("requester_id", "id")
w_csat_by_requester_id_running = Window.partitionBy("csat").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-14 03:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "csat",
        "tags",
    )
)

with_trends = (
    filtered_tickets
    .select(
        "*",
        F.count(F.lit(1)).over(w_agent_id_csat).alias("rows_in_agent_id_csat"),
        F.max("requester_id").over(w_agent_id_csat).alias("agent_id_csat_max_requester_id"),
        F.lead("requester_id").over(w_csat_by_requester_id).alias("next_requester_id"),
        F.sum("csat").over(w_csat_by_requester_id_running).alias("running_csat"),
    )
)

detail_rows = (
    with_trends.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.tags",
        "s.rows_in_agent_id_csat",
        "s.next_requester_id",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
    )
)

result = (
    detail_rows
    .groupBy("event_id", "agent_id")
    .pivot("event_type", ["replied", "resolved", "reopened", "escalated"])
    .agg(F.sum("next_requester_id"))
    .orderBy("event_id", "agent_id")
)
