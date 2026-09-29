from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("student_id") <= 10)
    .select(F.col("course_id").alias("ref_id"), F.col("grade_status").alias("name"))
)

course_rows = (
    df_courses
    .filter((F.col("credits") > 2) & (F.col("level") == "core"))
    .select("credits", "course_code")
)

result = (
    enrollment_side
    .intersectAll(course_rows)
    .orderBy("name", "ref_id")
)
