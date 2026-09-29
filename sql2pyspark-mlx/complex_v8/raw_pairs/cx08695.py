from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("grade_status") != "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "student_is_international",
    )
)
