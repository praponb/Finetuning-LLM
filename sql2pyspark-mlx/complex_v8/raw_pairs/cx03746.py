from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_events = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

with_prev = filtered_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_event_type_running))

result = (
    sessionized
    .groupBy("event_at_month", "ticket_id")
    .pivot("actor", ["customer", "system"])
    .agg(F.avg("is_new_session"))
    .orderBy("event_at_month", "ticket_id")
)
