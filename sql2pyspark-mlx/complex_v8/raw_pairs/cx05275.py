from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .select(
        F.col("course_id").alias("ref_id"),
        "grade_status",
        "id",
        F.lit("enrollment").alias("src"),
    )
)

course_rows = (
    df_courses
    .select(
        F.col("id").alias("ref_id"),
        F.col("course_code").alias("grade_status"),
        F.col("credits").alias("id"),
        F.lit("course").alias("src"),
    )
)

enrollment_rows_2 = (
    df_enrollments
    .filter(F.col("term") == "2024T1")
    .select(
        F.col("course_id").alias("ref_id"),
        "grade_status",
        F.col("student_id").alias("id"),
        F.lit("enrollment_3").alias("src"),
    )
)

result = (
    enrollment_rows
    .union(course_rows)
    .union(enrollment_rows_2)
)
