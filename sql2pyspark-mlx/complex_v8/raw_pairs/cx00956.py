from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term").orderBy("enrolled_at", "id")
w_term_running = Window.partitionBy("term").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

with_prev = cleaned.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_term))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_term_running))

sessions = (
    sessionized
    .groupBy("term", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

summary = (
    sessions
    .groupBy("session_no", "n_events")
    .agg(
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        F.percentile_approx("n_events", 0.5).alias("median_n_events"),
        F.sum(F.when(F.col("term").isNotNull(), F.col("n_events"))).alias("n_events_term_is_not_null"),
        F.min("n_events").alias("min_n_events"),
        F.round(F.sum("session_no") / F.nullif(F.sum("n_events"), F.lit(0)), 3).alias("session_no_to_n_events_ratio"),
    )
)

result = (
    summary
    .select(
        "session_no",
        "n_events_term_is_not_null",
        "min_n_events",
        "session_no_to_n_events_ratio",
    )
    .orderBy("n_events_term_is_not_null", F.col("min_n_events").asc())
)
