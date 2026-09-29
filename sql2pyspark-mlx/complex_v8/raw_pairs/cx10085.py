from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tickets = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-27 00:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

with_prev = valid_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "session_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

without_matches = (
    session_summary.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T1", "T3")) & (F.col("su.team") != "Tech"), "left_anti")
)

result = (
    without_matches
    .groupingSets(
        [["events", "session_id", "agent_id"], ["events"], ["session_id"], []],
        "events", "session_id", "agent_id",
    )
    .agg(F.min("session_start").alias("first_session_start"))
    .orderBy(
        F.col("events").asc_nulls_last(),
        F.col("session_id").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
