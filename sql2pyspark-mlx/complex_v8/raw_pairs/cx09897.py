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
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

result = (
    prepared
    .filter(F.col("grade_status_len") <= 10)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        (F.col("score") + F.col("grade_status_len")).alias("score_plus_grade_status_len"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
    .orderBy(F.col("score").desc(), F.col("student_id").asc())
)
