from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(9, 14))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
    )
)

course_id_stats = (
    cleaned
    .groupBy("course_id")
    .agg(F.avg("score_filled").alias("avg_score_filled"))
)

outliers = (
    cleaned.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score_filled") > F.col("g.avg_score_filled"))
    .select(
        "s.*",
        "g.avg_score_filled",
        F.round(F.col("s.score_filled") - F.col("g.avg_score_filled"), 2).alias("score_filled_minus_avg"),
    )
)

without_matches = (
    outliers.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort") == "C2023"), "left_semi")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

with_counts = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.avg_enrollment_score",
        "e.last_enrollment_enrolled_at",
        "e.total_enrollment_score",
    )
)

result = (
    with_counts
    .groupingSets(
        [["grade_status", "course_id", "student_id"], ["grade_status"]],
        "grade_status", "course_id", "student_id",
    )
    .agg(
        F.sum(F.when(F.col("grade_status") != "PASS", F.col("total_enrollment_score")).otherwise(0)).alias("total_enrollment_score_grade_status_pass"),
        F.round(F.avg("avg_enrollment_score"), 2).alias("avg_avg_enrollment_score"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "course_id",
        "student_id",
        "total_enrollment_score_grade_status_pass",
        "avg_avg_enrollment_score",
        "cnt",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
