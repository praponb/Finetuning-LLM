from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_course_id_running = Window.partitionBy("course_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_prev = recent_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_course_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_enrolled_at").isNull() | ((F.unix_timestamp("enrolled_at") - F.unix_timestamp("prev_enrolled_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_course_id_running))

per_session = (
    sessionized
    .groupBy("course_id", "session_id")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("score_filled").alias("session_score_filled"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

grouped = (
    per_session
    .groupBy(
        "session_id",
        F.year("session_end").alias("session_end_year"),
        F.when(F.col("duration_min") >= 168.0, "large").when(F.col("duration_min") >= 84.0, "medium").otherwise("small").alias("duration_min_tier"),
    )
    .agg(
        F.min("session_id").alias("min_session_id"),
        F.max("session_start").alias("last_session_start"),
        F.sum("n_events").alias("sum_n_events_check"),
    )
    .filter(F.col("sum_n_events_check") > 3)
    .drop("sum_n_events_check")
)

result = (
    grouped
    .select(
        "session_id",
        "session_end_year",
        "duration_min_tier",
        "min_session_id",
        F.substring(F.trim(F.col("duration_min_tier")), 1, 4).alias("duration_min_tier_prefix"),
    )
)
