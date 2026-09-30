from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

tagged = (
    recent_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

result = (
    tagged
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status_len",
    )
    .orderBy("course_id", F.col("student_id").desc_nulls_first())
)
