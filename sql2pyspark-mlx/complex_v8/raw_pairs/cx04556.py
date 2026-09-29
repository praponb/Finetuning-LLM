from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("agent_id").cast("string")).alias("channel_ref"),
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_agent_id_running))

per_session = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("requester_id").alias("min_ticket_requester_id"),
        F.count(F.when(F.col("priority") == "P3", 1)).alias("ticket_priority_count"),
        F.min("closed_at").alias("first_ticket_closed_at"),
    )
)

with_counts = (
    per_session.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        "t.min_ticket_requester_id",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        "t.first_ticket_closed_at",
    )
)

result = (
    with_counts
    .select(
        "agent_id",
        "session_id",
        "min_ticket_requester_id",
        "ticket_priority_count",
        "events",
    )
    .unpivot(
        ids=["agent_id", "session_id"],
        values=["min_ticket_requester_id", "ticket_priority_count", "events"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
