from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("term") != "2025T1") & (F.col("student_id") <= 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.when(F.col("score") >= 71.0, "HIGH").when(F.col("score") >= 38.3, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

filtered = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

derived = (
    filtered
    .filter(F.col("student_id") >= 4)
    .select(
        "*",
        (F.col("score") + F.col("grade_status_len")).alias("score_plus_grade_status_len"),
    )
)

result = (
    derived
    .select("id", "score", "score_plus_grade_status_len")
    .unpivot(
        ids=["id"],
        values=["score", "score_plus_grade_status_len"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
)
