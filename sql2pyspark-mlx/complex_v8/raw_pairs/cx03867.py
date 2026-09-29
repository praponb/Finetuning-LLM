from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_events = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = filtered_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_actor_running))

session_summary = (
    sessionized
    .groupBy("actor", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    session_summary
    .filter(F.col("actor") == "system")
    .select(
        "actor",
        "session_no",
        "session_start",
        "session_end",
        "duration_min",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.dayofweek(F.col("session_start")).alias("session_start_dayofweek"),
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

result = (
    labeled
    .cube("actor_group", "session_no")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.max("session_end").alias("last_session_end"),
    )
    .orderBy(
        F.col("actor_group").asc_nulls_last(),
        F.col("session_no").asc_nulls_last(),
    )
)
