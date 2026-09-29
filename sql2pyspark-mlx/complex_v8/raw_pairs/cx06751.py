from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "upgrade"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
    )
)

with_prev = recent.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
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

exploded_detail = (
    per_session.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (~F.col("t.channel").isin("email", "web")), "inner")
    .select(
        "s.agent_id",
        "s.burst_id",
        "s.duration_min",
        F.col("t.id").alias("ticket_id"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

result = (
    exploded_detail
    .filter(F.col("agent_id").isNotNull())
    .select(
        "agent_id",
        "burst_id",
        "duration_min",
        "ticket_id",
        "ticket_tags",
        "ticket_csat",
        F.initcap(F.trim(F.col("ticket_channel"))).alias("ticket_channel_clean"),
        F.size(F.col("ticket_tags")).alias("ticket_tags_count"),
    )
    .orderBy(F.col("agent_id").asc_nulls_last(), F.col("burst_id").asc(), "ticket_id")
    .limit(10)
)
