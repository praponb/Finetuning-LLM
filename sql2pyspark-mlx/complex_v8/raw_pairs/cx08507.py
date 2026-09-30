from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter(F.col("term").isin("2024T1", "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.abs(F.col("score") - 38.5).alias("score_distance"),
        F.when(F.col("score") >= 80.2, "HIGH").when(F.col("score") >= 20.2, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
    )
)

line_level = (
    scoped_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.score_distance",
        "s.grade_status_ref",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

enriched_course = (
    line_level.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.enrollment_course_id"), "inner")
    .filter(F.col("c.dept") == "Math")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.score_distance",
        "s.grade_status_ref",
        "s.enrollment_id",
        "s.enrollment_course_id",
        "s.enrollment_grade_status",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    enriched_course
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "score_distance",
        "grade_status_ref",
        "enrollment_id",
        "enrollment_course_id",
        "enrollment_grade_status",
        "course_credits",
        "course_level",
        "course_dept",
    )
    .orderBy(F.col("grade_status_ref").asc_nulls_last(), "enrollment_grade_status")
)
