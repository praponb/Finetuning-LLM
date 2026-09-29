from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
    )
)

with_prev = filtered_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_agent_id_running))

per_label = (
    sessionized
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "prev_closed_at",
        "new_session_flag",
        "burst_id",
        F.explode_outer("tags").alias("tag"),
    )
)

result = (
    per_label
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "prev_closed_at",
        "new_session_flag",
        "burst_id",
        F.hour(F.col("closed_at")).alias("closed_at_hour"),
    )
    .orderBy("agent_id", F.col("prev_closed_at").desc_nulls_first())
)
