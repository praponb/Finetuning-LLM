from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_duration_min = Window.partitionBy("agent_id").orderBy("duration_min", "agent_id", "session_id")
w_session_id = Window.partitionBy("session_id")
w_event_count_by_event_count_p = Window.partitionBy("event_count").orderBy(F.col("event_count").asc_nulls_last(), "agent_id", "session_id").rowsBetween(-6, -1)
w_agent_id_by_duration_min_2 = Window.partitionBy("agent_id").orderBy(F.col("duration_min").desc(), "agent_id", "session_id")

base = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp"))
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
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

with_parent_agent = (
    sessions.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.agent_id",
        "s.session_id",
        "s.session_start",
        "s.session_end",
        "s.event_count",
        "s.duration_min",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

with_trends = (
    with_parent_agent
    .select(
        "agent_id",
        "session_id",
        "session_start",
        "event_count",
        "agent_tier",
        "agent_hired_on",
        "agent_team",
        F.row_number().over(w_agent_id_by_duration_min).alias("rn"),
        F.sum("duration_min").over(w_session_id).alias("session_id_sum_duration_min"),
        F.sum("event_count").over(w_event_count_by_event_count_p).alias("event_count_sum_6_prior"),
        F.row_number().over(w_agent_id_by_duration_min_2).alias("rn_2"),
    )
)

result = (
    with_trends
    .select(
        "agent_id",
        "session_start",
        "event_count",
        "agent_tier",
        "agent_hired_on",
        "agent_team",
        "rn",
        "session_id_sum_duration_min",
        "event_count_sum_6_prior",
        "rn_2",
        F.quarter(F.col("session_start")).alias("session_start_quarter"),
    )
    .orderBy(F.col("session_start_quarter").desc())
)
