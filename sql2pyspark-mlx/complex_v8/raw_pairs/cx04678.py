from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") == "FAIL") | (F.col("student_id") >= 6))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
        F.when(F.col("score") >= 57.6, "large").when(F.col("score") >= 36.6, "medium").otherwise("small").alias("score_tier"),
    )
)

combined_details_course = (
    scoped_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_tier",
        F.col("c.credits").alias("course_credits"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    combined_details_course
    .groupingSets(
        [["course_id", "course_credits"], ["course_id"], ["course_credits"]],
        "course_id", "course_credits",
    )
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.min("student_gpa").alias("min_student_gpa"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("course_credits").asc_nulls_last(),
    )
)
