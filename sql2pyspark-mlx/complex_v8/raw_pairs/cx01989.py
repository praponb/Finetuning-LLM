from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("grade_status").isin("FAIL", "WITHDRAWN"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

annotated = (
    scoped
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_label",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

with_details_course = (
    annotated.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_label",
        "s.grade_status_len",
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

with_parent_course = (
    with_details_course.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept_2"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    with_parent_course
    .filter(~F.col("course_level").isin("advanced", "intro"))
    .select(
        "id",
        "term",
        "grade_status",
        "enrolled_at",
        "enrolled_at_label",
        "course_code",
        "course_dept",
        "course_level",
        "course_dept_2",
        "course_credits",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
    .orderBy(F.col("course_code").desc())
)
