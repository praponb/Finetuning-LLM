from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

with_prev = cleaned.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_actor_running))

result = (
    sessionized
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_ref",
        "prev_event_at",
        "session_no",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref_2"),
    )
    .orderBy(F.col("event_type_ref").asc_nulls_last(), "id")
    .limit(5)
)
