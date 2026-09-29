from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 10) & (F.col("score") <= 73.7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.count(F.when(F.col("grade_status") == "WITHDRAWN", 1)).alias("enrollment_grade_status_count"),
    )
)

summarized_rows = (
    filtered_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

with_details_course = (
    summarized_rows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", F.col("c.dept").alias("course_dept"), "c.course_code")
)

result = (
    with_details_course
    .filter(F.length(F.trim(F.col("course_code"))) >= 6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_or_unknown",
        "enrollment_grade_status_count",
        "course_code",
        (F.floor(F.col("score_bucket") / 25) * 25).alias("score_bucket_bucket"),
        F.abs(F.col("score") - 32.7).alias("score_distance"),
    )
    .orderBy(F.col("score_bucket_bucket").desc(), F.col("enrolled_at").asc())
)
