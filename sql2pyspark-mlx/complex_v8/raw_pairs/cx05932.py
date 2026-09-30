from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 4) & (F.col("term").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
    )
)

student_id_stats = (
    filtered_enrollments
    .groupBy("student_id")
    .agg(F.max("score").alias("max_score"))
)

above_average = (
    filtered_enrollments.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") == F.col("g.max_score"))
    .select(
        "s.*",
        "g.max_score",
        F.round(F.col("s.score") - F.col("g.max_score"), 2).alias("score_minus_max"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

with_stats = (
    above_average.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_stats
    .groupingSets(
        [["enrollment_count", "student_id", "term"], ["enrollment_count"], ["student_id"], []],
        "enrollment_count", "student_id", "term",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.col("term") != "2025T2", 1)).alias("n_term_2025t2"),
        F.percentile_approx("score", 0.5).alias("median_score"),
    )
    .orderBy(
        F.col("enrollment_count").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
    )
)
