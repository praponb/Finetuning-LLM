from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("course_id") <= 2) | (F.col("score") < 57.3))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_activity = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") > 11.1) & (F.col("e.term").isNotNull()), "left_semi")
)

grouped = (
    with_activity
    .groupBy("grade_status", "term")
    .agg(
        F.round(100 * F.count(F.when(F.col("grade_status") == "FAIL", 1)) / F.count("*"), 1).alias("pct_grade_status_fail"),
        F.count(F.when(F.col("grade_status").isNotNull(), 1)).alias("n_grade_status_is_not_null"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .filter(F.col("term") == "2024T2")
    .select(
        "grade_status",
        "n_grade_status_is_not_null",
        "row_count",
        F.round(F.col("row_count") / F.nullif(F.col("pct_grade_status_fail"), F.lit(0)), 3).alias("row_count_per_pct_grade_status_fail"),
    )
    .orderBy(F.col("row_count_per_pct_grade_status_fail").desc())
)
