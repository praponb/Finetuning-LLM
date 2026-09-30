from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "tags",
    )
)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_channel_running))

with_activity = (
    sessionized.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.closed_at") >= F.lit("2025-02-08 15:00:00").cast("timestamp")) & (~F.array_contains(F.col("t.tags"), "refund")), "left_semi")
)

labeled = (
    with_activity
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "tags",
        "prev_opened_at",
        "new_session_flag",
        "session_no",
        F.round(F.col("new_session_flag") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("new_session_flag_per_requester_id"),
        F.date_trunc("week", F.col("prev_opened_at")).alias("prev_opened_at_week"),
    )
)

result = (
    labeled
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "new_session_flag",
        "session_no",
        "prev_opened_at_week",
        F.array_contains(F.col("tags"), "outage").alias("has_outage"),
    )
)
