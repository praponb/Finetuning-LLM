from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("score") > 5.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

enrollment_side = scoped.select("student_id", F.col("enrolled_at").alias("event_time"))

enrollment_side_2 = df_enrollments.select("course_id", "enrolled_at")

result = (
    enrollment_side
    .union(enrollment_side_2)
    .distinct()
    .orderBy("student_id", "event_time")
)
