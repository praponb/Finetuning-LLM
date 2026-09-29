from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = scoped.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_actor_running))

per_session = (
    sessionized
    .groupBy("actor", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .select(
        "actor",
        "burst_id",
        "session_start",
        "events",
        "duration_min",
        F.year(F.col("session_end")).alias("session_end_year"),
        F.round(F.col("duration_min") / F.nullif(F.col("events"), F.lit(0)), 3).alias("duration_min_per_events"),
    )
    .orderBy(F.col("events").desc(), F.col("burst_id").asc_nulls_last(), "actor")
    .limit(5)
)
