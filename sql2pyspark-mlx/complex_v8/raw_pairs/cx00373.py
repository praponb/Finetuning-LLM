from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

with_prev = base.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_course_id_running))

per_session = (
    sessionized
    .groupBy("course_id", "burst_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    per_session
    .groupBy(
        "course_id",
        F.date_trunc("month", "session_start").alias("session_start_month"),
    )
    .agg(
        F.round(F.sum("burst_id") / F.nullif(F.sum("n_events"), F.lit(0)), 3).alias("burst_id_to_n_events_ratio"),
        F.percentile_approx("duration_min", 0.5).alias("median_duration_min"),
        F.max("session_start").alias("last_session_start"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.percentile_approx("n_events", 0.5).alias("median_n_events"),
        F.sum("n_events").alias("sum_n_events_check"),
    )
    .filter(F.col("sum_n_events_check") > 3)
    .drop("sum_n_events_check")
)

result = (
    per_group
    .select(
        "course_id",
        "session_start_month",
        "burst_id_to_n_events_ratio",
        "median_duration_min",
        "last_session_start",
        "distinct_course_ids",
        "median_n_events",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_session_start")).alias("days_since_last_session_start"),
    )
    .orderBy(F.col("median_n_events").desc_nulls_first())
)
