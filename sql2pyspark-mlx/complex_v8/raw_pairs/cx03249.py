from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) & (F.col("student_id") <= 4))
    .select(F.col("term").alias("label"), "id", F.lit("enrollment").alias("src"))
)

course_side = (
    df_courses
    .filter((F.col("credits") < 4) & (F.col("dept") != "Math"))
    .select("level", "id", F.lit("course").alias("src"))
)

enrollment_side_2 = (
    df_enrollments
    .select("grade_status", "student_id", F.lit("enrollment_3").alias("src"))
)

result = (
    enrollment_side
    .union(course_side)
    .union(enrollment_side_2)
    .orderBy("label")
)
