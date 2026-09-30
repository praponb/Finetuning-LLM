from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(7, 12)) & (F.col("term").isin("2025T2", "2024T2")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

joined_student = (
    filtered.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major"),
    )
)

result = (
    joined_student
    .groupBy("enrolled_at")
    .pivot("grade_status", ["FAIL", "PASS", "INCOMPLETE", "WITHDRAWN"])
    .agg(F.sum("grade_status_len"))
    .orderBy("enrolled_at")
)
