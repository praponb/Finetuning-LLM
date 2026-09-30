from pyspark.sql import functions as F

filtered_enrollments = df_enrollments

with_details_course = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") <= 3), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    with_details_course
    .groupBy("grade_status", "enrolled_at")
    .pivot("course_dept", ["Bio", "Biz"])
    .agg(F.sum("course_credits"))
)
