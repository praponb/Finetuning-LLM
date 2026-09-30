from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("grade_status") == "INCOMPLETE"))
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

joined_student = (
    recent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "st.student_name",
    )
)

line_level = (
    joined_student.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

derived = (
    line_level
    .select(
        "*",
        F.coalesce(F.col("enrollment_score"), F.lit(0.0)).alias("enrollment_score_filled"),
        (F.floor(F.col("enrollment_score") / 25) * 25).alias("enrollment_score_bucket"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
)

tagged = (
    derived
    .filter(~F.col("term").isin("2024T2", "2025T1"))
    .select(
        "*",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        (F.col("enrollment_score_filled") + F.col("enrollment_score")).alias("enrollment_score_filled_plus_enrollment_score"),
        F.concat_ws(" / ", F.col("term_clean"), F.col("term")).alias("term_clean_term_label"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
    )
)

profiled = (
    tagged.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        "e.first_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at_2",
    )
)

result = (
    profiled
    .filter(F.col("enrolled_at_date") >= F.lit("2024-08-01").cast("date"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_student_id",
        "enrollment_score_filled",
        "enrollment_score_bucket",
        "term_clean",
        "enrolled_at_date",
        "min_enrollment_score",
        "first_enrollment_enrolled_at",
    )
)
