from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .select(
        "course_id",
        F.col("student_id").alias("ref_id"),
        F.lit("enrollment").alias("source"),
    )
)

enrollment_rows_2 = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(10, 14))
    .select(
        F.col("id").alias("course_id"),
        F.col("student_id").alias("ref_id"),
        F.lit("enrollment_2").alias("source"),
    )
)

course_side = (
    df_courses
    .select(
        F.col("id").alias("course_id"),
        F.col("credits").alias("ref_id"),
        F.lit("course").alias("source"),
    )
)

result = (
    enrollment_rows
    .unionByName(enrollment_rows_2)
    .unionByName(course_side)
)
