from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("term") != "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("PASS", "FAIL"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

with_activity = (
    valid.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.dept") == "Bio"), "left_semi")
)

labeled = (
    with_activity
    .filter(F.col("student_id").isNotNull())
    .select(
        "*",
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
        F.abs(F.col("score") - 69.6).alias("score_distance"),
    )
)

combined_details_student = (
    labeled.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status_group",
        "s.enrolled_at_date",
        "s.grade_status_slug",
        "s.enrolled_at_plus_30m",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    combined_details_student
    .groupingSets(
        [["term", "student_id", "student_cohort"], ["term"], ["student_id"]],
        "term", "student_id", "student_cohort",
    )
    .agg(
        F.grouping("term").alias("is_total_term"),
        F.sum("student_gpa").alias("total_student_gpa"),
        F.min("enrolled_at_plus_30m").alias("first_enrolled_at_plus_30m"),
    )
)
