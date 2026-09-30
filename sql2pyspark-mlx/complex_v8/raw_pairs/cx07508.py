from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("term").isin("2025T1", "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

with_parent = (
    recent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at_quarter",
        F.col("st.is_international").alias("student_is_international"),
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

joined_course = (
    with_parent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at_quarter",
        "s.student_is_international",
        "s.course_code",
        F.col("c.course_code").alias("course_course_code"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level_2"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

scored = (
    joined_course
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at_quarter",
        "student_is_international",
        "course_code",
        "course_course_code",
        "course_dept",
        "course_level_2",
        "student_enrolled_on",
        F.month(F.col("student_enrolled_on")).alias("student_enrolled_on_month"),
    )
)

enriched = (
    scored.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.lower(F.col("st.student_name")).like("%ha%"))
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at_quarter",
        "s.student_is_international",
        "s.course_dept",
        "s.course_level_2",
        "s.student_enrolled_on",
        F.col("st.enrolled_on").alias("student_enrolled_on_2"),
        "st.student_name",
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    enriched
    .select(
        "id",
        "student_id",
        "enrolled_at_quarter",
        "course_dept",
        "student_enrolled_on",
        "student_enrolled_on_2",
        "student_name",
        "student_cohort",
        F.when(F.col("enrolled_at_quarter") >= 4, "HIGH").when(F.col("enrolled_at_quarter") >= 2, "MEDIUM").otherwise("LOW").alias("enrolled_at_quarter_tier"),
        F.date_trunc("week", F.col("student_enrolled_on_2")).alias("student_enrolled_on_2_week"),
    )
    .orderBy(F.col("course_dept").desc())
)
