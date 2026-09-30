from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        F.hour(F.col("opened_at")).alias("opened_at_hour"),
    )
)

with_prev = cleaned_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_priority_running))

filtered = (
    sessionized
    .crossJoin(sessionized.agg(F.avg("session_no").alias("overall_avg_session_no")))
    .filter(F.col("session_no") > F.col("overall_avg_session_no"))
    .drop("overall_avg_session_no")
)

labeled = (
    filtered
    .filter(F.month(F.col("closed_at")).isin(1, 3))
    .select(
        "*",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

result = (
    labeled
    .select(
        "id",
        "priority",
        "csat",
        "opened_at_hour",
        "prev_closed_at",
        "starts_session",
        F.substring(F.trim(F.col("priority_or_unknown")), 1, 2).alias("priority_or_unknown_prefix"),
    )
    .orderBy("opened_at_hour")
)
