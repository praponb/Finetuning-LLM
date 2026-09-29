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
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_event_type_running))

session_summary = (
    sessionized
    .groupBy("event_type", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

scored = (
    session_summary
    .filter(F.col("burst_id") > 2)
    .select(
        "event_type",
        "burst_id",
        "session_start",
        "duration_min",
        (F.col("session_start") + F.expr("INTERVAL 2 HOURS")).alias("session_start_plus_2h"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

labeled = (
    scored
    .select(
        "*",
        F.quarter(F.col("session_start_plus_2h")).alias("session_start_plus_2h_quarter"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        (F.col("session_start") + F.expr("INTERVAL 1 DAYS")).alias("session_start_plus_1d"),
    )
)

result = (
    labeled
    .groupBy("session_start_plus_2h_quarter")
    .pivot("event_type_or_unknown", ["replied", "UNKNOWN", "resolved", "escalated"])
    .agg(F.sum("event_type_len"))
)
