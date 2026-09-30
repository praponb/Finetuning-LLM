from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = prepared.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_event_type_running))

sessions = (
    sessionized
    .groupBy("event_type", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    sessions
    .select(
        "*",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.greatest(F.col("duration_min"), F.col("session_no")).alias("greatest_duration_min_session_no"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

annotated = (
    enriched
    .select(
        "event_type",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "event_type_len",
        "greatest_duration_min_session_no",
        "event_type_slug",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown_2"),
        F.dayofweek(F.col("session_end")).alias("session_end_dayofweek"),
    )
)

result = (
    annotated
    .groupingSets(
        [["event_type_or_unknown", "event_type", "session_end_dayofweek"], ["event_type_or_unknown"], ["event_type"]],
        "event_type_or_unknown", "event_type", "session_end_dayofweek",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("session_end_dayofweek").alias("min_session_end_dayofweek"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("event_type_or_unknown").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("session_end_dayofweek").asc_nulls_last(),
    )
)
