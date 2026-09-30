from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.col("gpa") < 3.9)
    .select(
        "id",
        F.col("student_name").alias("name"),
        F.col("enrolled_on").alias("event_date"),
        F.lit("student").alias("record_type"),
    )
)

student_rows = (
    df_students
    .filter((F.col("major").isin("Biz", "Math", "CS")) & (F.upper(F.trim(F.col("student_name"))).like("C%")))
    .select(
        "id",
        F.col("student_name").alias("name"),
        F.col("enrolled_on").alias("event_date"),
        F.lit("student_2").alias("record_type"),
    )
)

result = student_side.union(student_rows)
