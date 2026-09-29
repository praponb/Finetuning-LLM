from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

enrollment_side = (
    prepared
    .select(
        F.col("grade_status").alias("name"),
        "id",
        F.lit("enrollment").alias("origin"),
    )
)

student_side = (
    df_students
    .filter((F.col("is_international")) & (F.upper(F.trim(F.col("student_name"))).like("C%")))
    .select("cohort", "id", F.lit("student").alias("origin"))
)

enrollment_side_2 = (
    df_enrollments
    .filter((F.col("term") == "2024T1") & (F.col("grade_status") == "FAIL"))
    .select("grade_status", "id", F.lit("enrollment_3").alias("origin"))
)

result = (
    enrollment_side
    .union(student_side)
    .union(enrollment_side_2)
)
