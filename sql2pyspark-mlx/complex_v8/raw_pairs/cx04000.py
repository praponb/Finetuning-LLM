from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end = Window.orderBy("session_end", "agent_id", "session_id")
w_events = Window.partitionBy("events")

scoped = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.substring(F.trim(F.col("channel")), 1, 4).alias("channel_prefix"),
    )
)

with_prev = scoped.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_agent_id_running))

sessions = (
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

ranked = (
    sessions
    .select(
        "agent_id",
        "session_id",
        "session_start",
        "session_end",
        "events",
        "session_csat",
        "duration_min",
        F.dense_rank().over(w_all_by_session_end).alias("session_end_dense_rank"),
        F.count(F.lit(1)).over(w_events).alias("rows_in_events"),
        F.size(F.collect_set("agent_id").over(w_events)).alias("distinct_agent_id_in_events"),
        (F.col("duration_min") - F.lag("duration_min").over(w_all_by_session_end)).alias("duration_min_change"),
    )
)

result = (
    ranked
    .groupingSets(
        [["session_id", "events"], ["session_id"], ["events"], []],
        "session_id", "events",
    )
    .agg(
        F.percentile_approx("session_csat", 0.5).alias("median_session_csat"),
        F.sum("distinct_agent_id_in_events").alias("total_distinct_agent_id_in_events"),
        F.min("session_start").alias("first_session_start"),
    )
)
