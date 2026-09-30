from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_or_unknown = Window.partitionBy("priority_or_unknown").orderBy("closed_at", "id")
w_priority_or_unknown_running = Window.partitionBy("priority_or_unknown").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
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
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

with_prev = base_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority_or_unknown))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_priority_or_unknown_running))

result = (
    sessionized
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "priority_or_unknown",
        "prev_closed_at",
        "session_id",
    )
)
