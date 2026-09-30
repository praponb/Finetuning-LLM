from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") != "WITHDRAWN")
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

enrollment_side = (
    base_enrollments
    .filter(F.col("student_id") <= 11)
    .select(
        F.col("course_id").alias("ref_id"),
        F.col("id").alias("entity_id"),
        F.col("enrolled_at").alias("event_time"),
        F.lit("enrollment").alias("source"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "enrolled_at",
        F.lit("enrollment_2").alias("source"),
    )
)

result = enrollment_side.union(enrollment_side_2)
