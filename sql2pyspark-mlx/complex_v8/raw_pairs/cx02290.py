from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
    )
)

with_prev = recent_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_agent_id_running))

per_session = (
    sessionized
    .groupBy("agent_id", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    per_session
    .crossJoin(per_session.agg(F.max("duration_min").alias("overall_max_duration_min")))
    .filter(F.col("duration_min") < F.col("overall_max_duration_min"))
    .drop("overall_max_duration_min")
)

result = (
    filtered
    .select(
        "agent_id",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "session_requester_id",
        "duration_min",
        F.greatest(F.col("duration_min"), F.col("session_no")).alias("greatest_duration_min_session_no"),
    )
    .orderBy(F.col("agent_id").desc(), "session_no")
    .limit(10)
)
