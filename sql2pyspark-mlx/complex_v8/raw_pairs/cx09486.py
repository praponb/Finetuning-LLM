from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

with_prev = cleaned.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_ticket_id_running))

result = (
    sessionized
    .filter(F.col("event_type") == "reopened")
    .select(
        "id",
        "event_type",
        "actor",
        "event_type_slug",
        "prev_event_at",
        "starts_session",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
    .orderBy(
        F.col("actor").desc_nulls_first(),
        F.col("event_type_slug").asc_nulls_last(),
    )
)
