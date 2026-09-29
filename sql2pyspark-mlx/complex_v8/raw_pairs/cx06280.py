from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.col("course_id").alias("key_id"),
        "enrolled_at",
        F.lit("enrollment").alias("record_type"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("course_id").isNotNull()))
    .select("student_id", "enrolled_at", F.lit("enrollment_2").alias("record_type"))
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .distinct()
    .orderBy(F.col("enrolled_at").desc())
)
