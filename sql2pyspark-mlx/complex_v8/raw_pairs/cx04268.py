from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

with_prev = filtered.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_ticket_id_running))

sessions = (
    sessionized
    .groupBy("ticket_id", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

scored = (
    sessions
    .select(
        "ticket_id",
        "session_no",
        "session_end",
        "event_count",
        "duration_min",
        F.least(F.col("session_no"), F.col("event_count")).alias("least_session_no_event_count"),
        F.date_trunc("day", F.col("session_end")).alias("session_end_day"),
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 60, 1).alias("minutes_session_end_to_session_start"),
    )
)

result = (
    scored
    .select(
        "ticket_id",
        "session_no",
        "event_count",
        "duration_min",
        "least_session_no_event_count",
        "session_end_day",
        "minutes_session_end_to_session_start",
        F.coalesce(F.col("event_count"), F.lit(0)).alias("event_count_filled"),
        F.round(F.col("duration_min") * 0.25, 2).alias("duration_min_adj"),
    )
    .orderBy("event_count", "ticket_id", "session_no")
    .limit(10)
)
