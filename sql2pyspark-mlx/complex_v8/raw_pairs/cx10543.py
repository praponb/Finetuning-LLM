from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
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

joined_course = (
    cleaned_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.dept").alias("course_dept"),
    )
)

grouped = (
    joined_course
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_score"),
        F.max("enrolled_at").alias("last_enrolled_at"),
    )
)

enriched = (
    grouped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.enrolled_on") >= F.lit("2024-04-01").cast("date"))
    .select(
        "s.student_id",
        "s.min_score",
        "s.last_enrolled_at",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    enriched
    .groupBy("last_enrolled_at", "student_id")
    .pivot("student_cohort", ["C2024", "C2023"])
    .agg(F.sum("min_score"))
    .orderBy("last_enrolled_at", "student_id")
)
