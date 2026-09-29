from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)

with_prev = filtered.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_actor_running))

derived = (
    sessionized
    .filter(F.col("event_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "prev_event_at",
        "starts_session",
        "session_id",
        F.round((F.unix_timestamp(F.col("prev_event_at")) - F.unix_timestamp(F.col("event_at"))) / 60, 1).alias("minutes_event_at_to_prev_event_at"),
        F.hour(F.col("prev_event_at")).alias("prev_event_at_hour"),
        F.abs(F.col("starts_session") - 0).alias("starts_session_distance"),
    )
)

result = (
    derived
    .groupBy("starts_session")
    .pivot("actor", ["agent", "system", "customer"])
    .agg(F.sum("prev_event_at_hour"))
    .orderBy("starts_session")
)
