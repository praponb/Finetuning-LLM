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
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_event_type_running))

result = (
    sessionized
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "is_new_session",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
    .orderBy("is_new_session", "event_type")
)
