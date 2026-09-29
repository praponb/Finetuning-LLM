from pyspark.sql import functions as F

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.score").alias("prior_score_1d"),
    )
)

detail_rows_enrollments = (
    in_window.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_score_1d",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
    )
)

result = (
    detail_rows_enrollments
    .filter(F.col("enrollment_id").isNotNull())
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_score_1d",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_course_id",
        F.abs(F.col("enrollment_score") - 39.4).alias("enrollment_score_distance"),
        F.round(F.col("enrollment_score") / F.nullif(F.col("prior_score_1d"), F.lit(0)), 3).alias("enrollment_score_per_prior_score_1d"),
    )
    .orderBy(
        F.col("enrollment_score_distance").asc(),
        F.col("enrollment_score_per_prior_score_1d").asc_nulls_last(),
    )
)
