from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("closed_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
    )
)

with_prev = scoped_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_channel_running))

result = (
    sessionized
    .cube("channel", "burst_id")
    .agg(F.min("prev_closed_at").alias("first_prev_closed_at"))
    .select(
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        "burst_id",
        "first_prev_closed_at",
    )
    .orderBy(F.col("channel").asc_nulls_last(), F.col("burst_id").asc_nulls_last())
)
