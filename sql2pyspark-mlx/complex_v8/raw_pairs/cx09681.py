from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter((F.col("credits") < 3) & (F.upper(F.trim(F.col("course_code"))).like("B%")))
    .select("credits", "course_code")
)

enrollment_side = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select("student_id", "grade_status")
)

student_side = (
    df_students
    .filter((F.col("gpa") >= 2.7) & (F.col("enrolled_on").isNotNull()))
    .select("id", "major")
)

result = (
    course_side
    .union(enrollment_side)
    .union(student_side)
    .orderBy(F.col("course_code").desc())
)
