from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_prev = filtered_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_agent_id_running))

per_session = (
    sessionized
    .groupBy("agent_id", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .filter(F.col("session_start") < F.lit("2025-02-01 09:00:00").cast("timestamp"))
    .select(
        "agent_id",
        "session_start",
        "duration_min",
        F.round(F.col("event_count") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("event_count_per_duration_min"),
    )
)
