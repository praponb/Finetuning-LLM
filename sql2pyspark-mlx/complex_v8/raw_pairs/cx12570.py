from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
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

with_details = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
    )
)

result = (
    with_details
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "student_is_international",
        "student_name",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
    .orderBy("enrolled_at_label", "student_name", "id")
    .limit(20)
)
