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
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_event_type_running))

per_session = (
    sessionized
    .groupBy("event_type", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

without_matches = (
    per_session
    .crossJoin(per_session.agg(F.avg("duration_min").alias("overall_avg_duration_min")))
    .filter(F.col("duration_min") > F.col("overall_avg_duration_min"))
    .drop("overall_avg_duration_min")
)

result = (
    without_matches
    .groupingSets(
        [["events", "session_no", "event_type"], ["events"], ["session_no"], []],
        "events", "session_no", "event_type",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.col("event_type") != "escalated", 1)).alias("n_event_type_escalated"),
        F.round(F.sum("duration_min") / F.nullif(F.sum("session_no"), F.lit(0)), 3).alias("duration_min_to_session_no_ratio"),
        F.round(F.sum("session_no") / F.nullif(F.sum("duration_min"), F.lit(0)), 3).alias("session_no_to_duration_min_ratio"),
    )
)
