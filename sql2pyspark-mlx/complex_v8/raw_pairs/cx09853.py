from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
    )
)

with_prev = recent_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_actor_running))

session_summary = (
    sessionized
    .groupBy("actor", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

qualified = (
    session_summary
    .crossJoin(session_summary.agg(F.avg("event_count").alias("overall_avg_event_count")))
    .filter(F.col("event_count") > F.col("overall_avg_event_count"))
    .drop("overall_avg_event_count")
)

without_matches = (
    qualified
    .crossJoin(qualified.agg(F.avg("burst_id").alias("overall_avg_burst_id")))
    .filter(F.col("burst_id") > F.col("overall_avg_burst_id"))
    .drop("overall_avg_burst_id")
)

result = (
    without_matches
    .select(
        "actor",
        "burst_id",
        "session_end",
        "event_count",
        F.concat(F.col("actor"), F.lit("#"), F.col("burst_id").cast("string")).alias("actor_ref"),
    )
    .orderBy(F.col("session_end").asc_nulls_last(), F.col("actor_ref").desc())
)
