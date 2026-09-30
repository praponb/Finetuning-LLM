from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

with_prev = base_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_ticket_id_running))

derived = (
    sessionized
    .filter(F.col("session_id") >= 3)
    .select(
        "*",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.when(F.col("session_id") >= 4, "A").when(F.col("session_id") >= 2, "B").otherwise("C").alias("session_id_tier"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

scored = (
    derived
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_slug",
        "starts_session",
        "session_id",
        "event_type_clean",
        "session_id_tier",
        "actor_len",
        F.round((F.unix_timestamp(F.col("prev_event_at")) - F.unix_timestamp(F.col("event_at"))) / 3600, 1).alias("hours_event_at_to_prev_event_at"),
        F.coalesce(F.col("session_id_tier"), F.lit("UNKNOWN")).alias("session_id_tier_or_unknown"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

result = (
    scored
    .select(
        "id",
        "event_at",
        "actor",
        "starts_session",
        "session_id",
        "actor_len",
        "hours_event_at_to_prev_event_at",
        "event_type_or_unknown",
    )
    .orderBy(F.col("actor").asc_nulls_last())
)
