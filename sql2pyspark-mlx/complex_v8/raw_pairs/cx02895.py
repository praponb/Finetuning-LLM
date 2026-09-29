from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-18 08:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

with_details_student = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    with_details_student
    .select(
        "id",
        F.col("score_adj").alias("m_score_adj"),
        F.col("score").alias("m_score"),
    )
    .unpivot(
        ids=["id"],
        values=["m_score_adj", "m_score"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric_name")
)
