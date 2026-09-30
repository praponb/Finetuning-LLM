from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("score") <= 63.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("score") >= 80.2, "HIGH").when(F.col("score") >= 16.3, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

summary = (
    valid_enrollments
    .groupBy("course_id")
    .agg(
        F.sum(F.when(F.col("term").isNotNull(), F.col("score"))).alias("score_term_is_not_null"),
        F.sum("score").alias("total_score"),
        F.count(F.when(F.col("grade_status") == "WITHDRAWN", 1)).alias("n_grade_status_withdrawn"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
    )
)

with_children_enrollments = (
    summary.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.course_id",
        "s.total_score",
        "s.n_grade_status_withdrawn",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
    )
)

result = (
    with_children_enrollments
    .filter(F.col("enrollment_term") == "2025T1")
    .select(
        "course_id",
        "total_score",
        "n_grade_status_withdrawn",
        "enrollment_id",
        "enrollment_score",
        "enrollment_enrolled_at",
        "enrollment_student_id",
        "enrollment_term",
    )
)
