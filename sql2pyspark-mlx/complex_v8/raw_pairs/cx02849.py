from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("score").between(2.2, 66.0))
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

enriched = (
    prepared_enrollments
    .withColumn("enrolled_at_year", F.date_trunc("year", F.col("enrolled_at")))
)

exploded_detail_enrollments = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at_year",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

rollup_base = (
    exploded_detail_enrollments
    .filter(F.col("enrollment_grade_status") == "WITHDRAWN")
    .groupBy("enrollment_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("enrollment_grade_status").isin("FAIL", "INCOMPLETE"), 1)) / F.count("*"), 1).alias("pct_enrollment_grade_status_in_f"),
        F.percentile_approx("score", 0.5).alias("median_score"),
    )
)

result = (
    rollup_base
    .filter(F.col("median_score") < 75.0)
    .select(
        "enrollment_id",
        "pct_enrollment_grade_status_in_f",
        "median_score",
        F.coalesce(F.col("median_score"), F.lit(0.0)).alias("median_score_filled"),
        F.coalesce(F.col("median_score"), F.lit(0.0)).alias("median_score_filled_2"),
    )
    .orderBy("median_score_filled")
)
