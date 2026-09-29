from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
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

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_channel_running))

result = (
    sessionized
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "prev_opened_at",
        "burst_id",
    )
    .orderBy("id")
)
