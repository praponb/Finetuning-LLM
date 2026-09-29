from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_prev = scoped.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_ticket_id_running))

sessions = (
    sessionized
    .groupBy("ticket_id", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("event_type_len").alias("session_event_type_len"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

tagged = (
    sessions
    .select(
        "*",
        F.coalesce(F.col("session_event_type_len"), F.lit(0)).alias("session_event_type_len_filled"),
        F.year(F.col("session_end")).alias("session_end_year"),
    )
)

result = (
    tagged
    .select(
        "ticket_id",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        "session_event_type_len_filled",
        "session_end_year",
    )
    .orderBy(
        F.col("event_count").desc(),
        F.col("session_end_year").desc_nulls_first(),
        "ticket_id",
        "session_id",
    )
    .limit(10)
)
