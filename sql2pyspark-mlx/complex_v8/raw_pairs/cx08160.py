from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("grade_status").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("score").alias("max_enrollment_score"),
        F.count("*").alias("enrollment_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    recent_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.max_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.last_enrollment_enrolled_at",
    )
)

totals = (
    summarized_rows
    .filter(F.col("course_id").isNotNull())
    .groupBy(
        F.when(F.col("score") >= 62.5, "large").when(F.col("score") >= 32.7, "medium").otherwise("small").alias("score_tier"),
        "grade_status",
    )
    .agg(
        F.sum(F.when(~F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE"), F.col("max_enrollment_score")).otherwise(0)).alias("max_enrollment_score_grade_status_not_in_withdra"),
        F.count(F.when(F.col("grade_status") == "FAIL", 1)).alias("n_grade_status_fail"),
        F.sum(F.when(F.col("grade_status") == "FAIL", F.col("max_enrollment_score"))).alias("max_enrollment_score_grade_status_fail"),
        F.round(F.stddev("enrollment_count"), 2).alias("stddev_enrollment_count"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
)

screened = (
    totals
    .crossJoin(totals.agg(F.min("distinct_grade_status").alias("overall_min_distinct_grade_status")))
    .filter(F.col("distinct_grade_status") > F.col("overall_min_distinct_grade_status"))
    .drop("overall_min_distinct_grade_status")
)

result = (
    screened
    .groupBy("n_grade_status_fail", "distinct_grade_status")
    .pivot("score_tier", ["medium", "small", "large"])
    .agg(F.min("stddev_enrollment_count").alias("min_v"), F.sum("stddev_enrollment_count").alias("sum_v"))
    .orderBy("n_grade_status_fail", "distinct_grade_status")
)
