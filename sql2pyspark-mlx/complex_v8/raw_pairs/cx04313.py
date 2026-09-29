from pyspark.sql import functions as F

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
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

detail_rows = (
    recent_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
    )
)

joined = (
    detail_rows.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("e.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.enrollment_enrolled_at",
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term_2"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("st.major").alias("student_major"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    joined
    .select(
        "id",
        "student_id",
        "course_id",
        "enrollment_id",
        "enrollment_term",
        "enrollment_enrolled_at",
        "enrollment_grade_status",
        "enrollment_term_2",
        "enrollment_student_id",
        "student_major",
        F.concat(F.col("enrollment_term_2"), F.lit("#"), F.col("course_id").cast("string")).alias("enrollment_term_2_ref"),
        F.concat(F.col("enrollment_term_2"), F.lit("#"), F.col("enrollment_student_id").cast("string")).alias("enrollment_term_2_ref_2"),
    )
    .orderBy("enrollment_grade_status", "id", "enrollment_id")
    .limit(20)
)
