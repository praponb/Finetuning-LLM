from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_session_start_mon = Window.partitionBy("event_type", F.month("session_start"))
w_event_type_session_no_by_ses = Window.partitionBy("event_type", "session_no").orderBy(F.col("session_start").desc())

recent = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
    )
)

with_prev = recent.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_event_type_running))

sessions = (
    sessionized
    .groupBy("event_type", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    sessions
    .select(
        "*",
        F.round(F.col("n_events") - F.avg("n_events").over(w_event_type_session_start_mon), 2).alias("n_events_vs_event_type_session_start_month_avg"),
        F.round(F.percent_rank().over(w_event_type_session_no_by_ses), 3).alias("session_start_percent_rank"),
        F.round(F.col("session_no") / F.nullif(F.sum("session_no").over(w_event_type_session_start_mon), F.lit(0)), 4).alias("session_no_share_of_event_type_session_start_mon"),
    )
)

result = (
    ranked
    .rollup("session_no", "event_type")
    .agg(
        F.percentile_approx("session_no", 0.5).alias("median_session_no"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
    .select(
        "session_no",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "median_session_no",
        "distinct_event_types",
    )
    .orderBy(
        F.col("session_no").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
