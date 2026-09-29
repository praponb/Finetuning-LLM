from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

enrollment_side = (
    prepared_enrollments
    .filter((F.col("grade_status").isin("FAIL", "WITHDRAWN", "INCOMPLETE")) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-12").cast("date")))
    .select(
        F.lit("enrollment").alias("source"),
        F.col("score_bucket").alias("ref_id"),
        F.col("score").alias("metric"),
    )
)

enrollment_rows = (
    df_enrollments
    .select(
        F.lit("enrollment_2").alias("source"),
        F.col("student_id").alias("ref_id"),
        F.col("score").alias("metric"),
    )
)

result = enrollment_side.unionByName(enrollment_rows)
