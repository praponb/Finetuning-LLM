from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter(F.col("csat").between(2, 4))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

with_prev = recent_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_channel_running))

screened = (
    sessionized.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.hour(F.col("t.event_at")).between(8, 10)) & (F.col("t.event_type") == "resolved"), "left_anti")
    .filter(F.col("csat").between(1, 5))
)

joined_agent = (
    screened.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-11-09").cast("date"))), "inner")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    joined_agent
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "prev_opened_at",
        "new_session_flag",
        "agent_hired_on",
        "agent_tier",
    )
    .orderBy(F.col("priority").desc())
)
