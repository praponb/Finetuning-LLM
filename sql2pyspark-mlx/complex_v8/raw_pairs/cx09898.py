from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

with_prev = base_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_priority_running))

per_session = (
    sessionized
    .groupBy("priority", "burst_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    per_session
    .crossJoin(per_session.agg(F.min("burst_id").alias("overall_min_burst_id")))
    .filter(F.col("burst_id") > F.col("overall_min_burst_id"))
    .drop("overall_min_burst_id")
)

without_matches = (
    filtered
    .crossJoin(filtered.agg(F.min("session_requester_id").alias("overall_min_session_requester_id")))
    .filter(F.col("session_requester_id") > F.col("overall_min_session_requester_id"))
    .drop("overall_min_session_requester_id")
)

result = (
    without_matches
    .filter(F.col("priority").isin("P3", "P4"))
    .select(
        "priority",
        "session_start",
        "session_requester_id",
        "duration_min",
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 60, 1).alias("minutes_session_end_to_session_start"),
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 60, 1).alias("minutes_session_end_to_session_start_2"),
    )
    .orderBy(
        F.col("priority").asc(),
        F.col("minutes_session_end_to_session_start").asc(),
    )
)
