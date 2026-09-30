from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .select(
        "course_id",
        F.col("enrolled_at").alias("happened_at"),
        F.lit("enrollment").alias("record_type"),
    )
)

enrollment_side = (
    df_enrollments
    .filter((F.col("course_id") <= 4) & (F.col("term") == "2025T2"))
    .select("course_id", "enrolled_at", F.lit("enrollment_2").alias("record_type"))
)

result = (
    enrollment_rows
    .union(enrollment_side)
    .distinct()
)
