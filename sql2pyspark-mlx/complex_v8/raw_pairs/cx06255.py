from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

with_details_course = (
    base_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.upper(F.trim(F.col("c.course_code"))).like("C%"))
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    with_details_course
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "course_credits",
        "course_dept",
        F.coalesce(F.col("course_credits"), F.lit(0)).alias("course_credits_filled"),
    )
    .orderBy(F.col("course_credits_filled").desc(), "id")
    .limit(10)
)
