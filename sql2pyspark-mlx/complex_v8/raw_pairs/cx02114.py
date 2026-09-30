from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.count(F.when(F.col("grade_status").isin("WITHDRAWN", "FAIL"), 1)).alias("enrollment_grade_status_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.last_enrollment_enrolled_at",
    )
)

with_children = (
    with_counts.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_term_label",
        "s.total_enrollment_score",
        "s.enrollment_grade_status_count",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

result = (
    with_children
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "total_enrollment_score",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_student_id",
        F.length(F.trim(F.col("grade_status_term_label"))).alias("grade_status_term_label_len"),
        F.round((F.unix_timestamp(F.col("enrolled_at")) - F.unix_timestamp(F.col("enrollment_enrolled_at"))) / 60, 1).alias("minutes_enrollment_enrolled_at_to_enrolled_at"),
    )
)
