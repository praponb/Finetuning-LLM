from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("grade_status") == "PASS") & (F.col("student_id") >= 19))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 55.6).alias("score_distance"),
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_counts = (
    base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

result = (
    with_counts
    .groupingSets(
        [["grade_status", "student_id", "term"], ["grade_status"], ["student_id"]],
        "grade_status", "student_id", "term",
    )
    .agg(
        F.grouping("grade_status").alias("is_total_grade_status"),
        F.round(F.sum("score_adj") / F.nullif(F.sum("score_distance"), F.lit(0)), 3).alias("score_adj_to_score_distance_ratio"),
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
    )
)
