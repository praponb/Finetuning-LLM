from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("score") > 10.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at_2"),
        F.when(F.col("grade_status").isin("INCOMPLETE", "PASS"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

filtered = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

line_level_enrollments = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") < 79.2), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.days_since_enrolled_at",
        "s.days_since_enrolled_at_2",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    line_level_enrollments
    .groupingSets(
        [["student_id", "enrollment_grade_status"], ["student_id"], ["enrollment_grade_status"], []],
        "student_id", "enrollment_grade_status",
    )
    .agg(
        F.sum(F.when(F.col("enrollment_grade_status") == "WITHDRAWN", F.col("score")).otherwise(0)).alias("score_enrollment_grade_status_with"),
        F.sum(F.when(F.col("enrollment_grade_status").isNotNull(), 1).otherwise(0)).alias("n_enrollment_grade_status_is_n"),
    )
)
