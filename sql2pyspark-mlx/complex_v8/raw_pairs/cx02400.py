from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tickets
    .filter(F.col("priority") != "P4")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
    )
)

with_prev = scoped.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_priority_running))

result = (
    sessionized
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        "prev_opened_at",
        "new_session_flag",
        "burst_id",
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
        F.abs(F.col("burst_id") - 4).alias("burst_id_distance"),
    )
    .orderBy("new_session_flag", F.col("burst_id_distance").asc_nulls_last())
)
