from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.col("student_id") >= 16) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("-", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
        "st.student_name",
    )
)

result = (
    with_parent
    .groupingSets(
        [["grade_status", "student_id", "term"], ["grade_status"], ["student_id"], []],
        "grade_status", "student_id", "term",
    )
    .agg(
        F.grouping("grade_status").alias("is_total_grade_status"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
)
