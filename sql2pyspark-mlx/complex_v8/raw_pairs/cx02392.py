from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .filter(F.col("csat") > 2)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

with_prev = valid_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

screened = (
    sessions.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel").isin("chat", "web")) & (F.col("t.priority").isNotNull()), "left_semi")
    .filter(F.col("session_csat").between(7, 15))
)

result = (
    screened
    .filter(F.col("agent_id") <= 9)
    .select(
        "agent_id",
        "burst_id",
        "session_start",
        "session_end",
        "event_count",
        "session_csat",
        "duration_min",
    )
    .orderBy(F.col("session_end").desc())
)
