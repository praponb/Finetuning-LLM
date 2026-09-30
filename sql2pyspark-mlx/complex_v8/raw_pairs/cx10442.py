from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_requester_id_tier = Window.partitionBy("requester_id_tier").orderBy("closed_at", "id")
w_requester_id_tier_running = Window.partitionBy("requester_id_tier").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter(F.col("requester_id") >= 7)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        F.when(F.col("requester_id") >= 17, "HIGH").when(F.col("requester_id") >= 7, "MEDIUM").otherwise("LOW").alias("requester_id_tier"),
    )
)

with_prev = recent_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_requester_id_tier))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_requester_id_tier_running))

per_session = (
    sessionized
    .groupBy("requester_id_tier", "session_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    per_session
    .select(
        "*",
        (F.col("session_end") + F.expr("INTERVAL 2 HOURS")).alias("session_end_plus_2h"),
        F.dayofweek(F.col("session_end")).alias("session_end_dayofweek"),
        (F.col("n_events") + F.col("duration_min")).alias("n_events_plus_duration_min"),
    )
)

result = (
    labeled
    .select(
        "requester_id_tier",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        "n_events_plus_duration_min",
        F.to_date(F.col("session_end_plus_2h")).alias("session_end_plus_2h_date"),
    )
)
