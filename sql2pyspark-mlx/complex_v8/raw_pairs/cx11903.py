from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("score") <= 59.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 15 MINUTES")).alias("enrolled_at_plus_15m"),
    )
)

qualified = (
    recent_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (~F.col("e.grade_status").isin("WITHDRAWN", "FAIL")), "left_anti")
)

combined_details_course = (
    qualified.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

detail_rows = (
    combined_details_course.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_plus_15m",
        "s.course_level",
        "s.course_dept",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

result = (
    detail_rows
    .groupBy("course_id")
    .pivot("course_level", ["core", "intro"])
    .agg(F.avg("score"))
    .orderBy("course_id")
)
