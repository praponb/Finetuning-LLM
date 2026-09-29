from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_starts_session_agent_id_by_s = Window.partitionBy("starts_session", "agent_id").orderBy("starts_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id_priority = Window.partitionBy("burst_id", "priority")

scoped = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
    )
)

with_prev = scoped.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_agent_id_running))

sequenced = (
    sessionized
    .withColumn("first_known_starts_session", F.first("starts_session", ignorenulls=True).over(w_starts_session_agent_id_by_s))
    .withColumn("burst_id_priority_max_burst_id", F.max("burst_id").over(w_burst_id_priority))
)

result = (
    sequenced
    .select(
        "id",
        "starts_session",
        "burst_id_priority_max_burst_id",
        "requester_id",
    )
    .unpivot(
        ids=["id"],
        values=["starts_session", "burst_id_priority_max_burst_id", "requester_id"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
