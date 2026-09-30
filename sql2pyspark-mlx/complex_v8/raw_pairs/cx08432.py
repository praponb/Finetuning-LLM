from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
        F.count("*").alias("enrollment_count_3"),
    )
)

summarized_rows = (
    valid_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
        F.coalesce(F.col("e.enrollment_count_3"), F.lit(0)).alias("enrollment_count_3"),
    )
)

tagged = (
    summarized_rows
    .select(
        "*",
        F.least(F.col("grade_status_len"), F.col("score_bucket")).alias("least_grade_status_len_score_bucket"),
        F.greatest(F.col("enrollment_count"), F.col("score_bucket")).alias("greatest_enrollment_count_score_bucket"),
    )
)

result = (
    tagged
    .rollup("course_id", "enrollment_count_3")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("grade_status_len") / F.nullif(F.sum("greatest_enrollment_count_score_bucket"), F.lit(0)), 3).alias("grade_status_len_to_greatest_enrollment_count_sc"),
    )
)
