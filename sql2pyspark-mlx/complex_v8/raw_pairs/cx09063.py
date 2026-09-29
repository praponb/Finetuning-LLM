from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_burst_id = Window.partitionBy("burst_id")
w_burst_id_by_first_session_en = Window.partitionBy("burst_id").orderBy("first_session_end", "n_events", "burst_id", "session_start_month")
w_burst_id_by_first_session_en_2 = Window.partitionBy("burst_id").orderBy("first_session_end", "n_events", "burst_id", "session_start_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = prepared.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_actor_running))

session_summary = (
    sessionized
    .groupBy("actor", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

stats = (
    session_summary
    .groupBy(
        "n_events",
        "burst_id",
        F.date_trunc("month", "session_start").alias("session_start_month"),
    )
    .agg(
        F.sum("duration_min").alias("total_duration_min"),
        F.round(F.sum("n_events") / F.nullif(F.sum("burst_id"), F.lit(0)), 3).alias("n_events_to_burst_id_ratio"),
        F.max("burst_id").alias("max_burst_id"),
        F.round(F.avg("n_events"), 2).alias("avg_n_events"),
        F.min("session_end").alias("first_session_end"),
        F.sum(F.when(F.col("actor") == "agent", F.col("n_events")).otherwise(0)).alias("n_events_actor_agent"),
    )
)

ranked = (
    stats
    .select(
        "*",
        F.round(F.col("max_burst_id") - F.avg("max_burst_id").over(w_burst_id), 2).alias("max_burst_id_vs_burst_id_avg"),
        F.lag("n_events_to_burst_id_ratio", 2).over(w_burst_id_by_first_session_en).alias("prev2_n_events_to_burst_id_ratio"),
        F.last("max_burst_id").over(w_burst_id_by_first_session_en_2).alias("last_max_burst_id"),
    )
)

result = (
    ranked
    .select(
        "n_events",
        "burst_id",
        "session_start_month",
        "total_duration_min",
        "n_events_to_burst_id_ratio",
        "avg_n_events",
        "first_session_end",
        "n_events_actor_agent",
        "max_burst_id_vs_burst_id_avg",
        "prev2_n_events_to_burst_id_ratio",
        "last_max_burst_id",
        (F.col("n_events_actor_agent") + F.col("n_events")).alias("n_events_actor_agent_plus_n_events"),
    )
    .orderBy(
        F.col("first_session_end").desc(),
        F.col("n_events_actor_agent_plus_n_events").desc(),
    )
)
