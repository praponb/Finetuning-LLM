from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("score") <= 77.0))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2025T1", "2024T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.term_group",
        F.col("c.dept").alias("course_dept"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id") <= 16) & (F.col("e.score") >= 42.4), "left_anti")
    .filter(F.col("student_id") >= 19)
)

tagged = (
    filtered
    .filter(F.col("student_id") <= 9)
    .select(
        "*",
        F.concat(F.col("term_group"), F.lit("#"), F.col("course_id").cast("string")).alias("term_group_ref"),
    )
)

result = (
    tagged
    .filter(F.col("term_group_ref").isNotNull())
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "course_dept",
        "student_enrolled_on",
        "term_group_ref",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("student_enrolled_on")).alias("days_since_student_enrolled_on"),
    )
    .orderBy("enrolled_at", F.col("days_since_student_enrolled_on").desc())
)
