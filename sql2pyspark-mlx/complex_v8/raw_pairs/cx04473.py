from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_ticket_events
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_actor_running))

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

totals = (
    per_session
    .groupBy(
        F.date_format("session_end", "yyyy-MM").alias("session_end_ym"),
        "session_id",
        "n_events",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("n_events"), 2).alias("avg_n_events"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.max("duration_min").alias("max_duration_min_check"),
    )
    .filter(F.col("max_duration_min_check") > 84.0)
    .drop("max_duration_min_check")
)

screened = (
    totals
    .crossJoin(totals.agg(F.min("session_id").alias("overall_min_session_id")))
    .filter(F.col("session_id") > F.col("overall_min_session_id"))
    .drop("overall_min_session_id")
)

result = (
    screened
    .filter(F.col("session_end_ym").isNotNull())
    .select(
        "session_end_ym",
        "n_events",
        "avg_n_events",
        F.round(F.col("avg_n_events") * 1.07, 2).alias("avg_n_events_adj"),
        (F.col("row_count") + F.col("distinct_actors")).alias("row_count_plus_distinct_actors"),
    )
)
