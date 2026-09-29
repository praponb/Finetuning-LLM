from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter(F.col("agent_id").isNotNull())
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
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
    )
)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_priority_running))

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("csat").alias("max_ticket_csat"),
        F.count("*").alias("ticket_count"),
        F.max("requester_id").alias("max_ticket_requester_id"),
    )
)

summarized_rows = (
    sessionized.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.max_ticket_csat",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.max_ticket_requester_id",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "requester_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        "tags",
        "channel_priority_label",
        "prev_opened_at",
        "starts_session",
        "burst_id",
        "max_ticket_csat",
        "max_ticket_requester_id",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("priority").desc())
)
