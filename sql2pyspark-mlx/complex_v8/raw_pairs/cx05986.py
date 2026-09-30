from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tickets
    .filter(F.col("agent_id") <= 2)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
    )
)

with_prev = cleaned.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_priority_running))

with_parent_agent = (
    sessionized.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.minutes_closed_at_to_opened_at",
        "s.is_new_session",
        "s.burst_id",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

result = (
    with_parent_agent
    .select(
        "id",
        "priority",
        "channel",
        "csat",
        "minutes_closed_at_to_opened_at",
        "is_new_session",
        "burst_id",
        "agent_hired_on",
        "agent_tier",
        "agent_name",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("agent_hired_on")).alias("days_since_agent_hired_on"),
    )
    .orderBy(F.col("csat").asc_nulls_last(), F.col("priority").desc())
)
