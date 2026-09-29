from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = df_tickets.filter(F.col("agent_id") <= 2)

with_prev = valid_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_channel_running))

result = (
    sessionized
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "tags",
        "prev_opened_at",
        "new_session_flag",
        F.substring(F.trim(F.col("priority")), 1, 3).alias("priority_prefix"),
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
    )
    .orderBy("closed_at", F.col("agent_id").desc_nulls_first())
)
