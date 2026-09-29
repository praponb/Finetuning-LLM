from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

result = (
    filtered_enrollments
    .filter(F.col("term") == "2024T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        F.concat_ws("-", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
    )
)
