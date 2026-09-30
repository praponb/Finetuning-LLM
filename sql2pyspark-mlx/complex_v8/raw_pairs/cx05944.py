from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id = Window.partitionBy("student_id").orderBy("enrolled_at", "id")
w_student_id_running = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_enrollments = (
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

with_prev = base_enrollments.withColumn("prev_enrolled_at", F.lag("enrolled_at").over(w_student_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_enrolled_at").isNull() | (F.col("enrolled_at") > F.col("prev_enrolled_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_student_id_running))

session_summary = (
    sessionized
    .groupBy("student_id", "session_no")
    .agg(
        F.min("enrolled_at").alias("session_start"),
        F.max("enrolled_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("score").alias("session_score"),
        F.round((F.unix_timestamp(F.max("enrolled_at")) - F.unix_timestamp(F.min("enrolled_at"))) / 60, 1).alias("duration_min"),
    )
)

line_level_enrollments = (
    session_summary.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score").between(5.6, 76.8)), "inner")
    .select(
        "s.student_id",
        "s.session_no",
        "s.session_start",
        "s.session_end",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

summary = (
    line_level_enrollments
    .groupBy("enrollment_id")
    .agg(
        F.min("session_start").alias("first_session_start"),
        F.round(F.sum("session_no") / F.nullif(F.sum("enrollment_score"), F.lit(0)), 3).alias("session_no_to_enrollment_score_ratio"),
        F.round(F.stddev("enrollment_score"), 2).alias("stddev_enrollment_score"),
        F.min("session_no").alias("min_session_no"),
        F.sum("session_no").alias("total_session_no"),
        F.max("session_end").alias("last_session_end"),
    )
)

result = (
    summary
    .select(
        "enrollment_id",
        "first_session_start",
        "session_no_to_enrollment_score_ratio",
        "stddev_enrollment_score",
        "min_session_no",
        "total_session_no",
        "last_session_end",
        F.hour(F.col("last_session_end")).alias("last_session_end_hour"),
    )
    .orderBy(F.col("last_session_end").desc(), "enrollment_id")
    .limit(5)
)
