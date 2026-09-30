from pyspark.sql import functions as F

student_rows = df_students.select(F.col("id").alias("ref_id"), "student_name")

course_side = (
    df_courses
    .filter(F.col("dept").isNotNull())
    .select(
        F.col("credits").alias("ref_id"),
        F.col("course_code").alias("student_name"),
    )
)

result = (
    student_rows
    .exceptAll(course_side)
    .orderBy("student_name")
)
