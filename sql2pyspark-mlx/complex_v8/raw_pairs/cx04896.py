from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

with_prev = scoped_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_actor_running))

sessions = (
    sessionized
    .groupBy("actor", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

rollup_base = (
    sessions
    .groupBy(F.to_date("session_end").alias("session_end_day"))
    .agg(
        F.countDistinct("actor").alias("distinct_actors"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.count("*").alias("n_rows"),
        F.max("burst_id").alias("max_burst_id_check"),
    )
    .filter(F.col("max_burst_id_check") > 2)
    .drop("max_burst_id_check")
)

result = (
    rollup_base
    .groupingSets(
        [["distinct_actors", "n_rows"], ["distinct_actors"], ["n_rows"]],
        "distinct_actors", "n_rows",
    )
    .agg(
        F.grouping("distinct_actors").alias("is_total_distinct_actors"),
        F.round(F.sum("n_rows") / F.nullif(F.sum("distinct_actors"), F.lit(0)), 3).alias("n_rows_to_distinct_actors_ratio"),
        F.min("session_end_day").alias("first_session_end_day"),
        F.min("distinct_actors").alias("min_distinct_actors"),
    )
    .orderBy(
        F.col("distinct_actors").asc_nulls_last(),
        F.col("n_rows").asc_nulls_last(),
    )
)
