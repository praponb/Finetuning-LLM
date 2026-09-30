from pyspark.sql import functions as F

cleaned_courses = (
    df_courses
    .filter((F.col("level").isin("advanced", "intro")) & (F.col("dept") != "Art"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

without_matches = (
    cleaned_courses.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.grade_status").isin("INCOMPLETE", "FAIL", "PASS")) & (F.col("e.student_id") >= 6), "left_anti")
    .filter(F.col("credits") < 3)
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at_2"),
    )
)

profiled = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        "e.last_enrollment_enrolled_at",
        "e.last_enrollment_enrolled_at_2",
    )
)

result = (
    profiled
    .groupBy("dept", "last_enrollment_enrolled_at_2")
    .pivot("level", ["advanced", "core"])
    .agg(F.sum("credits"))
)
