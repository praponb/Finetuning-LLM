from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter(F.col("requester_id") >= 2)
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
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

with_prev = scoped_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_channel_running))

with_details_agent = (
    sessionized.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.length(F.trim(F.col("su.agent_name"))) >= 5), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.channel_len",
        "s.burst_id",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

grouped = (
    with_details_agent
    .groupBy("channel", "csat", "agent_team")
    .agg(
        F.min("csat").alias("min_csat"),
        F.max("csat").alias("max_csat"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("burst_id"), 2).alias("stddev_burst_id"),
        F.sum("requester_id").alias("total_requester_id"),
    )
)

result = (
    grouped
    .select(
        "channel",
        "csat",
        "agent_team",
        "max_csat",
        "row_count",
        "stddev_burst_id",
        "total_requester_id",
        F.coalesce(F.col("agent_team"), F.lit("UNKNOWN")).alias("agent_team_or_unknown"),
    )
    .orderBy("csat")
)
