from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")) & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

combined_details_course = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

result = (
    combined_details_course
    .groupBy("student_id", "course_id")
    .pivot("course_dept", ["CS", "Biz"])
    .agg(F.count("student_gpa").alias("count_v"), F.sum("student_gpa").alias("sum_v"))
)
