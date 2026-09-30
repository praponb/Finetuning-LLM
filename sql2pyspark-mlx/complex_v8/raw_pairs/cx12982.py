from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .filter(F.col("agent_id") >= 9)
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

with_prev = prepared.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_priority_running))

qualified = (
    sessionized.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.requester_id") < 18) & (F.col("t.priority") != "P4"), "left_anti")
    .filter(F.array_contains(F.col("tags"), "upgrade"))
)

result = (
    qualified
    .filter(F.col("requester_id") <= 17)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "prev_closed_at",
        "is_new_session",
        "session_no",
        F.date_format(F.col("prev_closed_at"), "yyyy-MM").alias("prev_closed_at_label"),
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
    .orderBy(F.col("priority").desc())
)
