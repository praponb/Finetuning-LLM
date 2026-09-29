from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 15:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

with_details_course = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_label",
        "c.course_code",
        F.col("st.gpa").alias("student_gpa"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.count(F.when(F.col("term") == "2024T1", 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    with_details_course.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    summarized_rows
    .filter(F.col("student_gpa") < 3.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_label",
        "course_code",
        "student_gpa",
        "total_enrollment_score",
        "enrollment_term_count",
        "enrollment_count",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
    .orderBy(F.col("enrollment_count").desc_nulls_first(), "id")
    .limit(5)
)
