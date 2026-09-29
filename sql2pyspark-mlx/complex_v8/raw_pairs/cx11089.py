from pyspark.sql import functions as F

filtered_enrollments = df_enrollments.filter(F.col("term").isin("2025T2", "2024T1", "2024T2"))

joined_student = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    joined_student
    .select(
        "id",
        "grade_status",
        "enrolled_at",
        "student_enrolled_on",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
    .orderBy("enrolled_at", F.col("id").desc())
)
