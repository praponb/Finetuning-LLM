from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .filter(F.col("channel").isin("phone", "chat"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

with_prev = prepared.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_agent_id_running))

result = (
    sessionized
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "requester_id_filled",
        "prev_closed_at",
        "burst_id",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
    )
    .orderBy(F.col("burst_id").desc())
)
