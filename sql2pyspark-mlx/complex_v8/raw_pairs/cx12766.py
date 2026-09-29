from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

with_prev = prepared.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_event_type_running))

with_children = (
    sessionized.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.burst_id",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

tagged = (
    with_children
    .filter(F.col("burst_id") >= 2)
    .withColumn("event_event_at_label", F.date_format(F.col("event_event_at"), "yyyy-MM-dd"))
)

result = (
    tagged
    .select(
        "id",
        "ticket_id",
        "event_type",
        "burst_id",
        "event_id",
        "event_actor",
        "event_event_at",
        "event_event_at_label",
        F.date_trunc("hour", F.col("event_event_at")).alias("event_event_at_hour"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("event_id").cast("string")).alias("event_type_ref"),
    )
    .orderBy("burst_id", F.col("event_id").desc(), "id")
    .limit(10)
)
