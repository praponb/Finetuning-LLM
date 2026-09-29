from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
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

with_prev = prepared.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_agent_id_running))

result = (
    sessionized
    .select(
        "id",
        "priority",
        "channel",
        "csat",
        "prev_closed_at",
        "new_session_flag",
        "session_no",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev_closed_at")).alias("days_since_prev_closed_at"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("prev_closed_at")).alias("days_since_prev_closed_at_2"),
    )
    .orderBy("channel")
)
