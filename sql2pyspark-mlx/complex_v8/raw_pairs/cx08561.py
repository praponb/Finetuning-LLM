from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.array_contains(F.col("tags"), "login").alias("has_login"),
    )
)

with_prev = cleaned.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_channel_running))

result = (
    sessionized
    .groupBy("session_id", "agent_id")
    .pivot("channel", ["email", "web", "phone", "chat"])
    .agg(F.min("csat"))
)
