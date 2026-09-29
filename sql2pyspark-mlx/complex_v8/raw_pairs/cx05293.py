from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter((F.col("student_id") >= 19) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

with_parent = (
    base_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at_quarter",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at_quarter",
        "student_gpa",
        "course_credits",
        F.when(F.col("term").isin("2024T1", "2025T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
        F.abs(F.col("course_credits") - 3).alias("course_credits_distance"),
    )
    .orderBy(F.col("student_gpa").asc_nulls_last(), "course_credits")
)
