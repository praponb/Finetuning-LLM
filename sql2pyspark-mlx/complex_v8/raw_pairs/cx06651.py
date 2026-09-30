from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
    )
)

without_matches = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.to_date(F.col("e.enrolled_at")) < F.lit("2025-03-02").cast("date")), "left_anti")
)

joined_course = (
    without_matches.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_label",
        F.col("c.level").alias("course_level"),
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
        "s.enrolled_at",
        "s.course_credits",
        "s.course_dept",
        F.col("c.dept").alias("course_dept_2"),
    )
)

result = (
    with_parent_course
    .filter(F.col("course_credits") < 4)
    .select(
        "id",
        "course_id",
        "enrolled_at",
        "course_credits",
        "course_dept",
    )
    .orderBy("course_credits", "id")
    .limit(20)
)
