from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-27 00:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
    )
)

with_prev = filtered_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .groupingSets(
        [["agent_id", "burst_id", "n_events"], ["agent_id"], ["burst_id"]],
        "agent_id", "burst_id", "n_events",
    )
    .agg(
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.percentile_approx("n_events", 0.5).alias("median_n_events"),
        F.min("session_end").alias("first_session_end"),
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("burst_id").asc_nulls_last(),
        F.col("n_events").asc_nulls_last(),
    )
)
