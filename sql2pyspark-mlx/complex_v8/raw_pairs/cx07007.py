from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

with_prev = filtered.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_ticket_id_running))

sessions = (
    sessionized
    .groupBy("ticket_id", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .select(
        "ticket_id",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        F.date_format(F.col("session_end"), "yyyy-MM").alias("session_end_label"),
        F.round(F.col("burst_id") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("burst_id_per_duration_min"),
    )
    .orderBy(
        F.col("session_end").desc_nulls_first(),
        F.col("burst_id").asc(),
        "ticket_id",
    )
    .limit(20)
)
