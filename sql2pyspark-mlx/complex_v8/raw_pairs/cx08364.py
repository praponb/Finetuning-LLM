from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = prepared.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_event_type_running))

result = (
    sessionized
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "prev_event_at",
        "is_new_session",
        "session_id",
        F.coalesce(F.col("session_id"), F.lit(0)).alias("session_id_filled"),
        F.least(F.col("session_id"), F.col("is_new_session")).alias("least_session_id_is_new_session"),
    )
    .orderBy(F.col("event_type").asc_nulls_last(), "id")
    .limit(10)
)
