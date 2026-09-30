from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(6, 8)) & (F.col("grade_status") != "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

line_level_enrollments = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
    )
)

annotated = (
    line_level_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_term",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

without_matches = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("enrolled_at").alias("first_enrollment_enrolled_at"))
)

summarized_rows = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.enrollment_student_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at")
)

filtered_2 = (
    summarized_rows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.month(F.col("e.enrolled_at")).isin(1, 2)), "left_semi")
)

result = (
    filtered_2
    .filter(F.hour(F.col("first_enrollment_enrolled_at")).between(10, 15))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_term",
        "grade_status_len",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len_2"),
    )
    .orderBy(F.col("id").asc())
)
