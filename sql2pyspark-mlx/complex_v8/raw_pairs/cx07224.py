from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = recent_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_ticket_id_running))

event_type_stats = (
    sessionized
    .groupBy("event_type")
    .agg(F.avg("new_session_flag").alias("avg_new_session_flag"))
)

vs_group = (
    sessionized.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.new_session_flag") > F.col("g.avg_new_session_flag"))
    .select(
        "s.*",
        "g.avg_new_session_flag",
        F.round(F.col("s.new_session_flag") - F.col("g.avg_new_session_flag"), 2).alias("new_session_flag_minus_avg"),
    )
)

labeled = vs_group.withColumn("event_at_label", F.date_format(F.col("event_at"), "MMM yyyy"))

result = (
    labeled
    .filter(F.col("session_no") < 4)
    .select(
        "id",
        "ticket_id",
        "actor",
        "prev_event_at",
        "new_session_flag",
        "session_no",
        "avg_new_session_flag",
        "event_at_label",
    )
    .orderBy(F.col("session_no").desc(), F.col("id").desc())
)
