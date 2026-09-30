from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("term").isin("2025T1", "2025T2")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.abs(F.col("score") - 57.8).alias("score_distance"),
    )
)

filtered = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status").isNotNull()), "left_anti")
)

student_id_stats = (
    filtered
    .groupBy("student_id")
    .agg(F.max("score_distance").alias("max_score_distance"))
)

compared = (
    filtered.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score_distance") == F.col("g.max_score_distance"))
    .select(
        "s.*",
        "g.max_score_distance",
        F.round(F.col("s.score_distance") - F.col("g.max_score_distance"), 2).alias("score_distance_minus_max"),
    )
)

scored = (
    compared
    .filter(F.col("score_distance") > 30.0)
    .select(
        "*",
        F.abs(F.col("score_distance") - 35.0).alias("score_distance_distance"),
        (F.col("max_score_distance") - F.col("score_distance")).alias("max_score_distance_minus_score_distance"),
    )
)

screened = (
    scored.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (~F.col("e.term").isin("2025T1", "2025T2")), "left_anti")
    .filter(F.col("student_id").isNotNull())
)

result = (
    screened
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "grade_status_prefix",
        "grade_status_or_unknown",
        "max_score_distance",
        "score_distance_minus_max",
        F.round(F.col("max_score_distance") * 0.9, 2).alias("max_score_distance_adj"),
        F.greatest(F.col("score_distance_minus_max"), F.col("score_distance_distance")).alias("greatest_score_distance_minus_max_score_distance"),
    )
    .orderBy(
        F.col("greatest_score_distance_minus_max_score_distance").asc_nulls_last(),
    )
)
