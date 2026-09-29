from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = scoped_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_ticket_id_running))

scored = (
    sessionized
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "session_id",
        F.when(F.col("session_id") >= 4, "large").when(F.col("session_id") >= 2, "medium").otherwise("small").alias("session_id_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.greatest(F.col("session_id"), F.col("is_new_session")).alias("greatest_session_id_is_new_session"),
    )
)

result = (
    scored
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "session_id",
        "session_id_tier",
        "actor_slug",
        "greatest_session_id_is_new_session",
        F.coalesce(F.col("session_id"), F.lit(0)).alias("session_id_filled"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)
