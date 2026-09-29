from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

tagged = (
    prepared
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.abs(F.col("score") - 32.7).alias("score_distance"),
        F.when(F.col("score") >= 79.2, "HIGH").when(F.col("score") >= 32.9, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

result = (
    tagged
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "score_tier",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
    .orderBy(F.col("grade_status_len").desc())
)
