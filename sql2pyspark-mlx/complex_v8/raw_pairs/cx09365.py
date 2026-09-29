from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter(F.col("channel") == "chat")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
    )
)

with_prev = filtered_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_agent_id_running))

per_session = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

joined = (
    per_session.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", "su.agent_name")
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("channel") == "chat", 1)).alias("ticket_channel_count"),
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.count("*").alias("ticket_count"),
    )
)

with_stats = (
    joined.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        "t.first_ticket_closed_at",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

result = (
    with_stats
    .filter(F.col("agent_id") <= 6)
    .select(
        "agent_id",
        "session_start",
        "session_requester_id",
        "duration_min",
        "agent_name",
        "ticket_channel_count",
        "first_ticket_closed_at",
        "ticket_count",
    )
    .orderBy(F.col("duration_min").asc(), F.col("ticket_channel_count").desc())
)
