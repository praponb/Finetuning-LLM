from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_events = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(8, 14))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = valid_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_event_type_running))

sessions = (
    sessionized
    .groupBy("event_type", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    sessions
    .filter(F.col("duration_min").between(60.0, 180.0))
    .select(
        "event_type",
        "session_id",
        "session_end",
        "event_count",
        "duration_min",
        F.date_format(F.col("session_start"), "yyyy-MM").alias("session_start_label"),
    )
)

result = (
    labeled
    .select(
        "event_type",
        "session_id",
        "session_end",
        "event_count",
        "duration_min",
        "session_start_label",
        F.lower(F.trim(F.col("session_start_label"))).alias("session_start_label_clean"),
    )
    .orderBy(F.col("session_start_label").desc_nulls_first())
)
