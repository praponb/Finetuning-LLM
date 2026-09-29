from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter(F.col("requester_id") >= 7)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
    )
)

with_prev = filtered_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_agent_id_running))

per_session = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .filter(F.col("session_end") < F.lit("2025-02-01 09:00:00").cast("timestamp"))
    .select(
        "agent_id",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        "session_csat",
        "duration_min",
    )
    .orderBy(F.col("session_id").asc_nulls_last(), "agent_id")
    .limit(20)
)
