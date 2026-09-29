from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("score") <= 59.8)
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

enrollment_side = (
    prepared_enrollments
    .filter((F.col("grade_status_len") < 10) & (F.col("course_id").isNotNull()))
    .select(F.col("grade_status").alias("label"))
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("course_id") >= 5)
    .select("term")
)

result = (
    enrollment_side
    .subtract(enrollment_rows)
    .orderBy("label")
)
