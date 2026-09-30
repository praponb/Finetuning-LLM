from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(8, 14)) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_details_student = (
    base_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "st.student_name",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    with_details_student
    .groupingSets(
        [["student_is_international", "course_credits"], ["student_is_international"], []],
        "student_is_international", "course_credits",
    )
    .agg(
        F.grouping("student_is_international").alias("is_total_student_is_international"),
        F.countDistinct("term").alias("distinct_terms"),
    )
    .orderBy(
        F.col("student_is_international").asc_nulls_last(),
        F.col("course_credits").asc_nulls_last(),
    )
)
