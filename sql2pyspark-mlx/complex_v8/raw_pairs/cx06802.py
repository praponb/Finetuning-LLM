from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term") == "2024T2", 1)).alias("enrollment_term_count"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

with_details_student = (
    with_counts.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select("s.*", "st.student_name")
)

result = (
    with_details_student
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "enrollment_term_count",
        "student_name",
    )
    .orderBy(F.col("id").desc())
)
