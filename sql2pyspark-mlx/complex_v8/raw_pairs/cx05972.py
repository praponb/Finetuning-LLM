from pyspark.sql import functions as F

filtered_enrollments = df_enrollments

combined_details = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.dept").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    combined_details
    .groupBy("enrolled_at")
    .pivot("grade_status", ["INCOMPLETE", "PASS", "WITHDRAWN"])
    .agg(F.max("course_credits"))
    .orderBy("enrolled_at")
)
