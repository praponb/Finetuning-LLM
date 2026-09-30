from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.length(F.trim(F.col("student_name"))) >= 7)
    .select("major", "enrolled_on")
)

student_rows = (
    df_students
    .filter((F.upper(F.trim(F.col("student_name"))).like("A%")) & (F.col("cohort").isNotNull()))
    .select("student_name", "enrolled_on")
)

result = student_side.union(student_rows)
