from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

with_prev = scoped_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_event_type_running))

sessions = (
    sessionized
    .groupBy("event_type", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    sessions
    .crossJoin(sessions.agg(F.max("n_events").alias("overall_max_n_events")))
    .filter(F.col("n_events") < F.col("overall_max_n_events"))
    .drop("overall_max_n_events")
)

result = (
    filtered
    .filter(F.col("duration_min") >= 60.0)
    .select(
        "event_type",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
    )
    .orderBy(
        "session_end",
        F.col("duration_min").desc_nulls_first(),
        "event_type",
        "burst_id",
    )
    .limit(20)
)
