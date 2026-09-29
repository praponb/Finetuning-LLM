from pyspark.sql import functions as F

student_side = (
    df_students
    .select(
        F.lit("student").alias("source"),
        F.col("id").alias("entity_id"),
        F.col("enrolled_on").alias("event_date"),
    )
)

student_side_2 = (
    df_students
    .filter(F.year(F.col("enrolled_on")) == 2025)
    .select(F.lit("student_2").alias("source"), "id", "enrolled_on")
)

result = (
    student_side
    .union(student_side_2)
    .distinct()
    .orderBy("source")
)
