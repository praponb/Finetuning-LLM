from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
        "csat",
        "tags",
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
    )
)

with_prev = recent_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_priority_running))

result = (
    sessionized
    .select(
        "id",
        F.col("is_new_session").alias("is_new_session_v"),
        F.col("csat").alias("csat_v"),
        F.col("requester_id").alias("requester_id_v"),
    )
    .unpivot(
        ids=["id"],
        values=["is_new_session_v", "csat_v", "requester_id_v"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
)
