from pyspark.sql import functions as F

recent = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

line_level = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.enrolled_at") < F.lit("2025-03-08 07:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.course_code",
        "s.level",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

joined_enrollment = (
    line_level.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.enrollment_student_id"), "left")
    .select(
        "s.id",
        "s.course_code",
        "s.level",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.enrollment_student_id",
        F.col("e.score").alias("enrollment_score_2"),
        "st.student_name",
        F.col("st.is_international").alias("student_is_international"),
    )
)

with_parent = (
    joined_enrollment.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.enrollment_student_id"), "inner")
    .select(
        "s.id",
        "s.course_code",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.enrollment_student_id",
        "s.student_is_international",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international_2"),
    )
)

result = (
    with_parent
    .groupBy("enrollment_student_id", "enrollment_id")
    .pivot("student_cohort", ["C2022", "C2024", "C2021", "C2023"])
    .agg(F.max("enrollment_score"))
)
