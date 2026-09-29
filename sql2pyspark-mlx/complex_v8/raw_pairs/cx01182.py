from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
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
        F.date_trunc("month", F.col("closed_at")).alias("closed_at_month"),
    )
)

with_prev = cleaned.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_agent_id_running))

labeled = (
    sessionized
    .filter(F.col("requester_id") > 8)
    .select(
        "*",
        F.abs(F.col("session_id") - 4).alias("session_id_distance"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at_month")).alias("days_since_closed_at_month"),
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
    )
)

result = (
    labeled
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "session_id",
        "session_id_distance",
        "days_since_closed_at_month",
    )
    .orderBy("opened_at")
)
