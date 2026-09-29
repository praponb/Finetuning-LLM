from pyspark.sql import functions as F

valid = df_enrollments.filter((F.col("score") < 76.8) & (F.hour(F.col("enrolled_at")).between(9, 12)))

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("score").alias("min_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
    )
)

summarized_rows = (
    valid.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.min_enrollment_score",
        "e.first_enrollment_enrolled_at_2",
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["student_id", "grade_status"], ["student_id"], ["grade_status"], []],
        "student_id", "grade_status",
    )
    .agg(
        F.round(F.sum("score") / F.nullif(F.sum("min_enrollment_score"), F.lit(0)), 3).alias("score_to_min_enrollment_score_ratio"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
