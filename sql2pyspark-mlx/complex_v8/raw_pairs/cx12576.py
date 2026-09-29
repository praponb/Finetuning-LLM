from pyspark.sql import functions as F

student_rows = df_students.select("cohort")

course_side = (
    df_courses
    .filter(F.col("dept").isNotNull())
    .select(F.col("dept").alias("cohort"))
)

result = (
    student_rows
    .subtract(course_side)
    .orderBy("cohort")
)
