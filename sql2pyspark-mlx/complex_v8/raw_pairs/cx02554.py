from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter(F.col("agent_id") >= 9)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "csat",
        "tags",
    )
)

with_prev = filtered_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_agent_id_running))

agent_id_stats = (
    sessionized
    .groupBy("agent_id")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

above_average = (
    sessionized.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "starts_session",
        "session_no",
        "avg_requester_id",
        "requester_id_minus_avg",
    )
)
