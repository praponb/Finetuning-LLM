from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.array_contains(F.col("tags"), "bug").alias("has_bug"),
    )
)

with_prev = valid_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_agent_id_running))

result = (
    sessionized
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "has_bug",
        "prev_opened_at",
        "is_new_session",
        "session_id",
        F.array_contains(F.col("tags"), "bug").alias("has_bug_2"),
    )
    .orderBy(F.col("session_id").asc_nulls_last())
)
