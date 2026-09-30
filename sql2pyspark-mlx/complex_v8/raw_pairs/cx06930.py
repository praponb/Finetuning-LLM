from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end_prior_3 = Window.orderBy(F.col("session_end").asc_nulls_last(), "actor", "burst_id").rowsBetween(-3, -1)
w_actor_2 = Window.partitionBy("actor")
w_all_by_duration_min_running = Window.orderBy(F.col("duration_min").desc_nulls_first(), "actor", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end_full = Window.orderBy(F.col("session_end").asc_nulls_last(), "actor", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_session_end = Window.orderBy(F.col("session_end").asc_nulls_last(), "actor", "burst_id")

recent_events = df_ticket_events

with_prev = recent_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_actor_running))

per_session = (
    sessionized
    .groupBy("actor", "burst_id")
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
        "*",
        F.round(F.avg("duration_min").over(w_all_by_session_end_prior_3), 2).alias("duration_min_avg_3_prior"),
        F.round(F.avg("events").over(w_actor_2), 2).alias("actor_avg_events"),
        F.last("session_end", ignorenulls=True).over(w_all_by_duration_min_running).alias("last_known_session_end"),
        F.nth_value("session_end", 2).over(w_all_by_session_end_full).alias("second_session_end"),
        F.round((F.unix_timestamp("session_start") - F.unix_timestamp(F.lag("session_start").over(w_all_by_session_end))) / 60, 1).alias("minutes_since_prev_session_start"),
    )
)

result = (
    ranked
    .rollup("actor", "burst_id", "events")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum("actor_avg_events").alias("total_actor_avg_events"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("actor").isNotNull(), F.col("events"))).alias("events_actor_is_not_null"),
    )
    .select(
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "burst_id",
        "events",
        "gid",
        "total_actor_avg_events",
        "row_count",
        "events_actor_is_not_null",
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("burst_id").asc_nulls_last(),
        F.col("events").asc_nulls_last(),
    )
)
