from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("gpa").between(2.5, 3.2)) & (F.col("enrolled_on").isNotNull()))
    .select("id", F.col("cohort").alias("name"))
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select("course_id", "term")
)

result = student_side.intersectAll(enrollment_rows)
