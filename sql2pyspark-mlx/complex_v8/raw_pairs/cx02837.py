from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("score") > 8.7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

scored = (
    filtered_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .select(
        "*",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.when(F.col("grade_status").isin("INCOMPLETE", "PASS"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
    )
)

joined = (
    scored.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (~F.col("st.is_international")), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        "st.student_name",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
    )
)

with_details_student = (
    joined.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_prefix",
        "s.score_bucket",
        "s.term_or_unknown",
        "s.grade_status_group",
        "s.enrolled_at_label",
        "s.student_name",
        "s.course_credits",
        "s.course_code",
        F.col("st.student_name").alias("student_student_name"),
    )
)

result = (
    with_details_student
    .filter(~F.col("grade_status_group").isin("group_a", "group_b"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_or_unknown",
        "grade_status_group",
        "enrolled_at_label",
        "student_name",
        "course_credits",
        "course_code",
        "student_student_name",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label_2"),
    )
    .orderBy(F.col("enrolled_at_label_2").desc())
)
