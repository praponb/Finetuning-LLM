from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at_hour", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        F.date_trunc("hour", F.col("closed_at")).alias("closed_at_hour"),
    )
)

with_prev = cleaned_tickets.withColumn("prev_closed_at_hour", F.lag("closed_at_hour").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at_hour").isNull() | (F.col("closed_at_hour") > F.col("prev_closed_at_hour") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "session_no")
    .agg(
        F.min("closed_at_hour").alias("session_start"),
        F.max("closed_at_hour").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("closed_at_hour")) - F.unix_timestamp(F.min("closed_at_hour"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .select(
        "agent_id",
        "session_no",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.when(F.col("session_requester_id") >= 60, "HIGH").when(F.col("session_requester_id") >= 31, "MEDIUM").otherwise("LOW").alias("session_requester_id_tier"),
        (F.col("duration_min") - F.col("session_requester_id")).alias("duration_min_minus_session_requester_id"),
    )
    .orderBy("agent_id")
)
