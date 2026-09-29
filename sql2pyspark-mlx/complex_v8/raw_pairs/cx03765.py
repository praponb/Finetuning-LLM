from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 9) | (F.col("term").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

joined_course = (
    base_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.upper(F.trim(F.col("c.course_code"))).like("C%")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.days_since_enrolled_at",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

with_parent_course = (
    joined_course.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.course_credits",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept_2"),
    )
)

result = (
    with_parent_course
    .rollup("course_dept_2", "student_id", "course_credits")
    .agg(
        F.grouping("course_dept_2").alias("is_total_course_dept_2"),
        F.count(F.when(F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN", "FAIL"), 1)).alias("n_grade_status_in_incomplete_w"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("term").isNotNull(), F.col("score")).otherwise(0)).alias("score_term_is_not_null"),
    )
    .orderBy(
        F.col("course_dept_2").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("course_credits").asc_nulls_last(),
    )
)
