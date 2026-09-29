from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) | (F.col("score") >= 20.2))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.when(F.col("score") >= 57.6, "large").when(F.col("score") >= 20.2, "medium").otherwise("small").alias("score_tier"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

detail_rows_enrollments = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status_clean",
        "s.score_tier",
        "s.enrolled_at_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
    )
)

score_tier_stats = (
    detail_rows_enrollments
    .groupBy("score_tier")
    .agg(F.avg("score").alias("avg_score"))
)

vs_group = (
    detail_rows_enrollments.alias("s")
    .join(score_tier_stats.alias("g"), F.col("g.score_tier") == F.col("s.score_tier"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

tagged = (
    vs_group
    .filter(F.col("enrollment_term") != "2024T2")
    .select(
        "id",
        "student_id",
        "score",
        "grade_status_clean",
        "enrollment_id",
        "avg_score",
        "score_minus_avg",
        F.greatest(F.col("score_minus_avg"), F.col("score")).alias("greatest_score_minus_avg_score"),
    )
)

result = (
    tagged
    .filter(F.col("student_id") >= 2)
    .select(
        "id",
        "score",
        "grade_status_clean",
        "enrollment_id",
        "score_minus_avg",
        "greatest_score_minus_avg_score",
        F.length(F.trim(F.col("grade_status_clean"))).alias("grade_status_clean_len"),
    )
    .orderBy("enrollment_id")
)
