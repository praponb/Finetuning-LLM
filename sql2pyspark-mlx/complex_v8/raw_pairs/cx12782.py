from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
    )
)

with_prev = valid_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_agent_id_running))

joined = (
    sessionized.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", F.col("su.team").alias("agent_team"))
)

result = (
    joined
    .filter(F.col("csat") >= 1)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "prev_opened_at",
        "new_session_flag",
        "session_id",
        "agent_team",
    )
    .orderBy("prev_opened_at", F.col("agent_id").desc())
)
