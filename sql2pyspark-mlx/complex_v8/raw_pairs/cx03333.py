from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_prev = cleaned.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "session_no")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    sessions
    .select(
        "agent_id",
        "session_no",
        "session_start",
        "session_end",
        "session_csat",
        "duration_min",
        (F.col("session_end") + F.expr("INTERVAL 15 MINUTES")).alias("session_end_plus_15m"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(~F.col("priority").isin("P4", "P1"), 1)).alias("ticket_priority_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

profiled = (
    annotated.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        "t.last_ticket_closed_at",
    )
)

result = (
    profiled
    .select(
        "agent_id",
        "session_no",
        "session_start",
        "session_csat",
        "duration_min",
        "session_end_plus_15m",
        "ticket_priority_count",
        "last_ticket_closed_at",
        F.dayofweek(F.col("session_start")).alias("session_start_dayofweek"),
        F.date_trunc("year", F.col("last_ticket_closed_at")).alias("last_ticket_closed_at_year"),
    )
    .orderBy("last_ticket_closed_at")
)
