from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        "course_id",
        F.col("grade_status").alias("label"),
        F.lit("enrollment").alias("src"),
    )
)

enrollment_side_2 = df_enrollments.select("student_id", "term", F.lit("enrollment_2").alias("src"))

result = (
    enrollment_side
    .union(enrollment_side_2)
    .orderBy("label", "src")
)
