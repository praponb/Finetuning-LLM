from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter((F.col("score") >= 36.6) & (F.hour(F.col("enrolled_at")).between(8, 14)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_parent = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

result = (
    with_parent
    .groupBy("course_level")
    .pivot("grade_status", ["WITHDRAWN", "INCOMPLETE", "FAIL"])
    .agg(F.avg("score"))
)
