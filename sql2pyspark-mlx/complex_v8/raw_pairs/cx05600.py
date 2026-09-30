from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("score") > 38.5)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

detail_rows_enrollments = (
    prepared_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at_label",
        "s.grade_status_slug",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

with_stats = (
    detail_rows_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_children_enrollments = (
    with_stats.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_slug",
        "s.enrollment_id",
        "s.first_enrollment_enrolled_at",
        "s.enrollment_count",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_counts = (
    with_children_enrollments.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

line_level = (
    with_counts.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status").isin("INCOMPLETE", "WITHDRAWN")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_slug",
        "s.enrollment_id",
        "s.first_enrollment_enrolled_at",
        "s.enrollment_id_2",
        "s.enrollment_enrolled_at",
        "s.enrollment_course_id",
        "s.last_enrollment_enrolled_at",
        F.col("e.id").alias("enrollment_id_3"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

with_parent = (
    line_level.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_slug",
        "s.enrollment_id",
        "s.first_enrollment_enrolled_at",
        "s.enrollment_id_2",
        "s.enrollment_enrolled_at",
        "s.enrollment_course_id",
        "s.last_enrollment_enrolled_at",
        "s.enrollment_id_3",
        "s.enrollment_grade_status",
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

result = (
    with_parent
    .filter(F.col("enrollment_id") <= 82)
    .select(
        "id",
        "student_id",
        "grade_status_slug",
        "enrollment_id",
        "enrollment_id_2",
        "enrollment_enrolled_at",
        "enrollment_course_id",
        "last_enrollment_enrolled_at",
        "enrollment_id_3",
        "enrollment_grade_status",
        "enrollment_grade_status_2",
        "enrollment_term",
        F.to_date(F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_date"),
        F.concat_ws(": ", F.col("grade_status_slug"), F.col("enrollment_grade_status")).alias("grade_status_slug_enrollment_grade_status_label"),
    )
    .orderBy(
        F.col("grade_status_slug").desc(),
        F.col("enrollment_enrolled_at").asc(),
        "id",
        "enrollment_id",
        "enrollment_id_2",
        "enrollment_id_3",
    )
    .limit(5)
)
