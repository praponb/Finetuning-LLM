from pyspark.sql import functions as F

valid_courses = (
    df_courses
    .filter((F.col("level").isNotNull()) | (F.col("credits") > 2))
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.concat_ws("|", F.col("course_code"), F.col("level")).alias("course_code_level_label"),
        F.substring(F.trim(F.col("level")), 1, 3).alias("level_prefix"),
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
    )
)

without_matches = (
    valid_courses.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.score") > 5.9) & (F.to_date(F.col("e.enrolled_at")) < F.lit("2025-03-14").cast("date")), "left_anti")
)

line_level_enrollments = (
    without_matches.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.student_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.dept",
        "s.course_code_level_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count(F.when(F.col("term") != "2025T1", 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    line_level_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.enrollment_student_id"), "left")
    .select(
        "s.*",
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_counts
    .groupBy("enrollment_id", "enrollment_student_id")
    .pivot("dept", ["Art", "CS", "Biz"])
    .agg(F.count("avg_enrollment_score").alias("count_v"), F.sum("avg_enrollment_score").alias("sum_v"))
)
