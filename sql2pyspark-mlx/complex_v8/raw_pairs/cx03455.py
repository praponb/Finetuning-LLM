from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter(F.col("score") >= 32.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
    )
)

with_parent = (
    cleaned.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.score_adj",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "student_gpa",
        F.round(F.col("score") / F.nullif(F.col("score_adj"), F.lit(0)), 3).alias("score_per_score_adj"),
    )
    .orderBy(F.col("student_gpa").desc(), F.col("term").asc())
)
