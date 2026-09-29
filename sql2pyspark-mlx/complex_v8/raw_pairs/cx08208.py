from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
    )
)

with_prev = valid_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    sessions.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.opened_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")) & (F.size(F.col("t.tags")) >= 1), "left_anti")
)

joined = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team") == "VIP"), "inner")
    .select(
        "s.agent_id",
        "s.burst_id",
        "s.session_end",
        "s.event_count",
        "s.session_csat",
        "s.duration_min",
        "su.agent_name",
    )
)

result = (
    joined
    .select(
        "agent_id",
        "burst_id",
        "session_end",
        "event_count",
        "session_csat",
        "duration_min",
        F.coalesce(F.col("session_csat"), F.lit(0)).alias("session_csat_filled"),
        F.to_date(F.col("session_end")).alias("session_end_date"),
    )
    .orderBy(
        F.col("session_end").desc(),
        F.col("burst_id").asc_nulls_last(),
        "agent_id",
    )
    .limit(10)
)
