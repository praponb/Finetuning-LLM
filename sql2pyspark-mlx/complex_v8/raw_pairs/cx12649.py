from pyspark.sql import functions as F

course_rows = df_courses.select("id", F.col("credits").alias("entity_id"), "dept")

enrollment_side = df_enrollments.select("student_id", "id", "grade_status")

course_side = (
    df_courses
    .filter(F.col("credits") < 4)
    .select("credits", "id", "dept")
)

result = (
    course_rows
    .union(enrollment_side)
    .union(course_side)
    .orderBy("id")
)
