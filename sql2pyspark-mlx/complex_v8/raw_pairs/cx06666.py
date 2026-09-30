from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tickets
    .filter(F.col("priority") != "P3")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
    )
)

with_prev = recent.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_agent_id_running))

enriched_agent = (
    sessionized.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.hired_on").between(F.lit("2024-01-30").cast("date"), F.lit("2025-03-30").cast("date")))
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.prev_closed_at",
        "s.session_no",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

detail_rows = (
    enriched_agent.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.prev_closed_at",
        "s.session_no",
        "s.agent_hired_on",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
    )
)

result = (
    detail_rows
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "prev_closed_at",
        "session_no",
        "agent_hired_on",
        "event_id",
        "event_type",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.month(F.col("agent_hired_on")).alias("agent_hired_on_month"),
    )
)
