from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter((F.month(F.col("enrolled_at")).isin(1, 3)) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.dept") != "CS"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.days_since_enrolled_at",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

with_parent_course = (
    enriched.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .filter(F.col("c.credits") <= 3)
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

result = (
    with_parent_course
    .select("id", "score", "student_gpa")
    .unpivot(
        ids=["id"],
        values=["score", "student_gpa"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
