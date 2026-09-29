from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .filter(F.col("event_type") != "replied")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = scoped.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_ticket_id_running))

result = (
    sessionized
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "prev_event_at",
        "new_session_flag",
        "session_no",
        F.round(F.col("new_session_flag") / F.nullif(F.col("session_no"), F.lit(0)), 3).alias("new_session_flag_per_session_no"),
        F.when(F.col("new_session_flag") >= 1, "A").when(F.col("new_session_flag") >= 0, "B").otherwise("C").alias("new_session_flag_tier"),
    )
    .orderBy("session_no", F.col("prev_event_at").desc(), "id")
    .limit(5)
)
