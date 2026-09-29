from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("student_id") <= 17)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 44.4).alias("score_distance"),
    )
)

enriched = valid.withColumn("score_distance_bucket", F.floor(F.col("score_distance") / 25) * 25)

joined_student = (
    enriched.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score_distance",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

filtered = (
    joined_student.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (~F.col("e.grade_status").isin("PASS", "INCOMPLETE")), "left_anti")
    .filter(F.col("student_cohort").isNotNull())
)

scored = (
    filtered
    .filter(F.col("score_distance") <= 85.0)
    .withColumn("least_score_distance_student_gpa", F.least(F.col("score_distance"), F.col("student_gpa")))
)

result = (
    scored
    .select(
        "id",
        "student_id",
        "course_id",
        "score_distance",
        "student_gpa",
        "student_cohort",
        "student_is_international",
        "least_score_distance_student_gpa",
        F.substring(F.trim(F.col("student_cohort")), 1, 2).alias("student_cohort_prefix"),
    )
)
