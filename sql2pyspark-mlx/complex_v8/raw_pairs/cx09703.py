from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

joined_student = (
    cleaned.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.enrolled_on") < F.lit("2025-07-03").cast("date")), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
        F.col("st.is_international").alias("student_is_international"),
    )
)

enriched = (
    joined_student.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.upper(F.trim(F.col("c.course_code"))).like("C%")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.student_enrolled_on",
        "s.student_is_international",
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

result = (
    enriched
    .groupingSets(
        [["grade_status", "student_is_international", "course_id"], ["grade_status"]],
        "grade_status", "student_is_international", "course_id",
    )
    .agg(F.grouping_id().alias("grouping_level"), F.count("*").alias("n_rows"))
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "student_is_international",
        "course_id",
        "grouping_level",
        "n_rows",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("student_is_international").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
