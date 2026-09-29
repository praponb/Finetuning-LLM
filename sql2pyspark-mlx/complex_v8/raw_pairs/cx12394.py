from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
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
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_priority_running))

result = (
    sessionized
    .groupBy("agent_id")
    .pivot("channel", ["web", "phone", "email", "chat"])
    .agg(F.sum("starts_session").alias("amount"), F.count("starts_session").alias("n"))
)
