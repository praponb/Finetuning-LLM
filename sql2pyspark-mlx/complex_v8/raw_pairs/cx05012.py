from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
    )
)

profiled = (
    cleaned_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.last_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at_2",
    )
)

grouped = (
    profiled
    .groupBy(
        F.to_date("first_enrollment_enrolled_at").alias("first_enrollment_enrolled_at_day"),
        "term",
        "grade_status",
    )
    .agg(
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.round(F.sum("score") / F.nullif(F.sum("grade_status_len"), F.lit(0)), 3).alias("score_to_grade_status_len_ratio"),
        F.sum(F.when(F.col("grade_status") == "FAIL", 1).otherwise(0)).alias("n_grade_status_fail"),
    )
)

without_matches = (
    grouped
    .crossJoin(grouped.agg(F.avg("n_grade_status_fail").alias("overall_avg_n_grade_status_fail")))
    .filter(F.col("n_grade_status_fail") > F.col("overall_avg_n_grade_status_fail"))
    .drop("overall_avg_n_grade_status_fail")
)

result = (
    without_matches
    .select(
        "first_enrollment_enrolled_at_day",
        "term",
        "grade_status",
        "term_list",
    )
)
