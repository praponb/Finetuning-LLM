from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_event_type_running))

session_summary = (
    sessionized
    .groupBy("event_type", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    session_summary
    .crossJoin(session_summary.agg(F.avg("session_no").alias("overall_avg_session_no")))
    .filter(F.col("session_no") > F.col("overall_avg_session_no"))
    .drop("overall_avg_session_no")
)

filtered_2 = (
    filtered
    .crossJoin(filtered.agg(F.max("duration_min").alias("overall_max_duration_min")))
    .filter(F.col("duration_min") < F.col("overall_max_duration_min"))
    .drop("overall_max_duration_min")
)

result = (
    filtered_2
    .select(
        "event_type",
        "session_no",
        "session_start",
        "event_count",
        "duration_min",
        F.greatest(F.col("duration_min"), F.col("session_no")).alias("greatest_duration_min_session_no"),
    )
)
