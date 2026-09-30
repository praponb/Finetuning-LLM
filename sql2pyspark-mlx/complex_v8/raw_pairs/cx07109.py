from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_actor_running))

sessions = (
    sessionized
    .groupBy("actor", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

totals = (
    sessions
    .groupBy("events", F.to_date("session_end").alias("session_end_day"), "actor")
    .agg(
        F.sum(F.when(F.col("actor") == "agent", F.col("duration_min"))).alias("duration_min_actor_agent"),
        F.count(F.when(F.col("actor") == "agent", 1)).alias("n_actor_agent"),
        F.max("duration_min").alias("max_duration_min"),
        F.sum("burst_id").alias("total_burst_id"),
        F.avg("burst_id").alias("avg_burst_id_check"),
    )
    .filter(F.col("avg_burst_id_check") > 2)
    .drop("avg_burst_id_check")
)

result = (
    totals
    .filter(F.col("events") <= 6)
    .select(
        "events",
        "session_end_day",
        "actor",
        "duration_min_actor_agent",
        "n_actor_agent",
        "max_duration_min",
        "total_burst_id",
        F.round(F.col("events") / F.nullif(F.col("n_actor_agent"), F.lit(0)), 3).alias("events_per_n_actor_agent"),
    )
    .orderBy(
        "n_actor_agent",
        "events",
        "session_end_day",
        "actor",
    )
    .limit(20)
)
