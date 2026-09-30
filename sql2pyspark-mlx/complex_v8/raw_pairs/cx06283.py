from pyspark.sql import functions as F

student_side = (
    df_students
    .select(
        F.lit("student").alias("record_type"),
        "id",
        F.col("cohort").alias("label"),
    )
)

course_rows = df_courses.select(F.lit("course").alias("record_type"), "id", "course_code")

result = (
    student_side
    .union(course_rows)
    .orderBy("record_type", "id")
)
