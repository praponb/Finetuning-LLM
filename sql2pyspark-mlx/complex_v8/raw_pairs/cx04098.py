from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = df_ticket_events

with_prev = scoped.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_event_type_running))

sessions = (
    sessionized
    .groupBy("event_type", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    sessions
    .select(
        "*",
        F.when(F.col("duration_min") >= 192.0, "large").when(F.col("duration_min") >= 60.0, "medium").otherwise("small").alias("duration_min_tier"),
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 60, 1).alias("minutes_session_start_to_session_end"),
        F.coalesce(F.col("session_id"), F.lit(0)).alias("session_id_filled"),
    )
)

result = (
    derived
    .groupBy("n_events")
    .pivot("duration_min_tier", ["large", "small", "medium"])
    .agg(F.sum("duration_min").alias("amount"), F.count("duration_min").alias("n"))
    .orderBy("n_events")
)
