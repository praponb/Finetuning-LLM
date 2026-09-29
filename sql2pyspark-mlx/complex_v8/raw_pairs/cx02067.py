from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

with_prev = prepared.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_actor_running))

per_session = (
    sessionized
    .groupBy("actor", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    per_session
    .select(
        "*",
        F.coalesce(F.col("events"), F.lit(0)).alias("events_filled"),
        F.coalesce(F.col("session_no"), F.lit(0)).alias("session_no_filled"),
        F.date_format(F.col("session_start"), "MMM yyyy").alias("session_start_label"),
    )
)

result = (
    annotated
    .cube("session_no", "actor", "events")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.count("*").alias("n_rows"),
    )
    .select(
        "session_no",
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "events",
        "grouping_level",
        "distinct_actors",
        "n_rows",
    )
    .orderBy(
        F.col("session_no").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
        F.col("events").asc_nulls_last(),
    )
)
