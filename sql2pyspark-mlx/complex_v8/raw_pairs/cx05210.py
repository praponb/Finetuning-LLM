from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tickets = (
    df_tickets
    .filter(F.col("agent_id") >= 8)
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
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
    )
)

with_prev = base_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_channel_running))

screened = (
    sessionized
    .crossJoin(sessionized.agg(F.avg("burst_id").alias("overall_avg_burst_id")))
    .filter(F.col("burst_id") > F.col("overall_avg_burst_id"))
    .drop("overall_avg_burst_id")
)

result = (
    screened
    .filter(F.col("opened_at") < F.lit("2025-02-12 14:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "opened_at_date",
        "prev_opened_at",
        "starts_session",
        "burst_id",
        F.least(F.col("starts_session"), F.col("csat")).alias("least_starts_session_csat"),
    )
    .orderBy(F.col("opened_at").asc_nulls_last())
)
