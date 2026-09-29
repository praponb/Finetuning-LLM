from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .filter(F.col("priority") == "P4")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
    )
)

with_prev = cleaned_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_agent_id_running))

result = (
    sessionized
    .select(
        "id",
        F.col("new_session_flag").alias("m_new_session_flag"),
        F.col("requester_id").alias("requester_id_v"),
        F.col("session_no").alias("m_session_no"),
    )
    .unpivot(
        ids=["id"],
        values=["m_new_session_flag", "requester_id_v", "m_session_no"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
