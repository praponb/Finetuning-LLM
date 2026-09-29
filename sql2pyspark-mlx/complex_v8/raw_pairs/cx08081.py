from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-07 11:00:00").cast("timestamp")) & (F.col("grade_status").isin("PASS", "INCOMPLETE", "WITHDRAWN")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

combined_details = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.upper(F.trim(F.col("st.student_name"))).like("A%"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_bucket",
        F.col("st.major").alias("student_major"),
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
    )
)

combined_details_2 = (
    combined_details.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_bucket",
        "s.student_major",
        "s.student_name",
        F.col("st.student_name").alias("student_student_name"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    combined_details_2
    .groupingSets(
        [["student_id", "course_credits"], ["student_id"], ["course_credits"], []],
        "student_id", "course_credits",
    )
    .agg(F.sum("score_bucket").alias("total_score_bucket"))
)
