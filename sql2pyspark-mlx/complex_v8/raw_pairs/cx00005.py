from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_events = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_prev = prepared_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_event_type_running))

enriched = (
    sessionized.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.days_since_event_at",
        "s.prev_event_at",
        "s.new_session_flag",
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.team").alias("agent_team"),
    )
)

event_type_stats = (
    enriched
    .groupBy("event_type")
    .agg(F.avg("days_since_event_at").alias("avg_days_since_event_at"))
)

compared = (
    enriched.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.days_since_event_at") > F.col("g.avg_days_since_event_at"))
    .select(
        "s.*",
        "g.avg_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.avg_days_since_event_at"), 2).alias("days_since_event_at_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "event_type",
        "days_since_event_at",
        "prev_event_at",
        "new_session_flag",
        "ticket_priority",
        "agent_team",
        "avg_days_since_event_at",
        "days_since_event_at_minus_avg",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("prev_event_at")).alias("days_since_prev_event_at"),
        F.quarter(F.col("prev_event_at")).alias("prev_event_at_quarter"),
    )
    .orderBy(F.col("agent_team").desc(), "id")
    .limit(5)
)
