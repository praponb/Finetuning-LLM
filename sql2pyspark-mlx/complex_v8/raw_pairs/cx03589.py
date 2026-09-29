from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        "course_id",
        F.col("term").alias("name"),
        "enrolled_at",
        F.lit("enrollment").alias("record_type"),
    )
)

enrollment_rows = (
    df_enrollments
    .select(
        "student_id",
        "grade_status",
        "enrolled_at",
        F.lit("enrollment_2").alias("record_type"),
    )
)

result = (
    enrollment_side
    .union(enrollment_rows)
    .distinct()
    .orderBy("record_type", "name")
)
