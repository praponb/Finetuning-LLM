from pyspark.sql import functions as F

recent_students = (
    df_students
    .filter((F.col("major").isin("Bio", "Math", "CS")) | (~F.col("is_international")))
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "is_international",
    )
)

student_side = (
    recent_students
    .filter((F.col("enrolled_on") >= F.lit("2024-04-05").cast("date")) & (F.upper(F.trim(F.col("student_name"))).like("J%")))
    .select(F.col("id").alias("entity_id"))
)

enrollment_side = df_enrollments.select("id")

result = student_side.subtract(enrollment_side)
