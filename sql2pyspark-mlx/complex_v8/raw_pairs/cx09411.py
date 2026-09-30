from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "grade_status",
        F.col("course_id").alias("ref_id"),
        F.lit("enrollment").alias("record_type"),
    )
)

student_side = df_students.select("major", "id", F.lit("student").alias("record_type"))

result = (
    enrollment_side
    .union(student_side)
    .distinct()
)
