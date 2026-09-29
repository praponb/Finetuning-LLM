from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id = Window.partitionBy("session_id")
w_session_id_events_by_duratio = Window.partitionBy("session_id", "events").orderBy(F.col("duration_min").desc_nulls_first(), "actor", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_session_id_events_by_duratio_2 = Window.partitionBy("session_id", "events").orderBy(F.col("duration_min").desc_nulls_first())

filtered_events = (
    df_ticket_events
    .filter(F.col("event_type") != "created")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_prev = filtered_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_actor_running))

per_session = (
    sessionized
    .groupBy("actor", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    per_session
    .select(
        "actor",
        "session_id",
        "session_start",
        "events",
        "duration_min",
        F.count(F.lit(1)).over(w_session_id).alias("rows_in_session_id"),
        F.first("events").over(w_session_id_events_by_duratio).alias("first_events"),
        F.round(F.percent_rank().over(w_session_id_events_by_duratio_2), 3).alias("duration_min_percent_rank"),
        F.size(F.collect_set("actor").over(w_session_id)).alias("distinct_actor_in_session_id"),
        F.max("duration_min").over(w_session_id).alias("session_id_max_duration_min"),
    )
)

result = (
    ranked
    .select(
        "actor",
        "session_id",
        "duration_min",
        "duration_min_percent_rank",
    )
    .unpivot(
        ids=["actor", "session_id"],
        values=["duration_min", "duration_min_percent_rank"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
