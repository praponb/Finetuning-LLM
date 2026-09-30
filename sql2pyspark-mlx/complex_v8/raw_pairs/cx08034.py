from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 80.2, "A").when(F.col("score") >= 20.6, "B").otherwise("C").alias("score_tier"),
        F.abs(F.col("score") - 57.8).alias("score_distance"),
    )
)

screened = (
    recent_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (~F.col("e.grade_status").isin("INCOMPLETE", "FAIL")), "left_anti")
    .filter(F.col("student_id").isNotNull())
)

qualified = (
    screened.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (~F.col("e.term").isin("2024T2", "2025T2")), "left_semi")
)

result = (
    qualified
    .groupBy("course_id", "enrolled_at")
    .pivot("grade_status", ["INCOMPLETE", "PASS"])
    .agg(F.min("score").alias("min_v"), F.count("score").alias("n"))
    .orderBy("course_id", "enrolled_at")
)
