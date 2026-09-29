from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(8, 14)) & (F.col("term") == "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("hour", F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

joined_student = (
    recent_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_hour",
        F.col("st.cohort").alias("student_cohort"),
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
    )
)

scored = (
    joined_student
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

result = (
    scored
    .rollup("grade_status", "course_id")
    .agg(
        F.count("*").alias("row_count"),
        F.min("days_since_enrolled_at").alias("min_days_since_enrolled_at"),
        F.max("enrolled_at_hour").alias("last_enrolled_at_hour"),
    )
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "course_id",
        "row_count",
        "min_days_since_enrolled_at",
        "last_enrolled_at_hour",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
