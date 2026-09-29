from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_n_events_by_session_en = Window.partitionBy("actor", "n_events").orderBy(F.col("session_end").desc())
w_session_end_month = Window.partitionBy(F.month("session_end"))

scoped = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = scoped.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_actor_running))

per_session = (
    sessionized
    .groupBy("actor", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    per_session
    .withColumn("session_end_percent_rank", F.round(F.percent_rank().over(w_actor_n_events_by_session_en), 3))
    .withColumn("session_end_month_min_duration_min", F.min("duration_min").over(w_session_end_month))
)

result = (
    windowed
    .groupingSets(
        [["session_id", "n_events"], ["session_id"], ["n_events"], []],
        "session_id", "n_events",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
        F.count("*").alias("row_count"),
        F.max("session_start").alias("last_session_start"),
    )
    .orderBy(
        F.col("session_id").asc_nulls_last(),
        F.col("n_events").asc_nulls_last(),
    )
)
