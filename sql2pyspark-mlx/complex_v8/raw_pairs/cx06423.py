from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

with_parent_student = (
    prepared_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.year(F.col("st.enrolled_on")) == 2025)
    .select(
        "s.*",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

derived = (
    with_parent_student
    .select(
        "*",
        F.when(F.col("term").isin("2024T2", "2024T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
)

result = (
    derived
    .select(
        "id",
        F.col("student_gpa").alias("student_gpa_v"),
        F.col("score").alias("score_v"),
    )
    .unpivot(
        ids=["id"],
        values=["student_gpa_v", "score_v"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
