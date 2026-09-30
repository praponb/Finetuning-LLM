from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("student_id") <= 8) | (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.sum("score").alias("total_enrollment_score"))
)

profiled = (
    scoped_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.total_enrollment_score")
)

labeled = (
    profiled
    .select(
        "id",
        "score",
        "grade_status",
        "enrolled_at_week",
        "total_enrollment_score",
        F.when(F.col("score") >= 58.8, "large").when(F.col("score") >= 13.9, "medium").otherwise("small").alias("score_tier"),
        (F.col("score") + F.col("total_enrollment_score")).alias("score_plus_total_enrollment_score"),
    )
)

scored = (
    labeled
    .select(
        "id",
        "grade_status",
        "score_tier",
        "score_plus_total_enrollment_score",
        F.when(F.col("grade_status").isin("FAIL", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

result = (
    scored
    .select(
        "id",
        "grade_status",
        "score_tier",
        "score_plus_total_enrollment_score",
        F.when(F.col("score_tier").isin("large", "small"), "group_a").when(F.col("score_tier") == "medium", "group_b").otherwise("other").alias("score_tier_group"),
        F.upper(F.trim(F.col("score_tier"))).alias("score_tier_clean"),
    )
    .orderBy(F.col("id").asc_nulls_last(), "score_plus_total_enrollment_score")
)
