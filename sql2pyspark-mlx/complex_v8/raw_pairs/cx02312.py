from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status") == "FAIL") & (F.col("e.student_id") >= 12), "left_semi")
)

labeled = (
    qualified
    .filter(F.col("score") > 32.9)
    .select(
        "id",
        "student_id",
        "score",
        "grade_status_ref",
        F.when(F.col("term").isin("2025T1", "2025T2"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
    )
)

enriched_student = (
    labeled.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select("s.*", F.col("st.gpa").alias("student_gpa"))
)

annotated = (
    enriched_student
    .withColumn("greatest_score_student_gpa", F.greatest(F.col("score"), F.col("student_gpa")))
)

result = (
    annotated
    .groupBy("student_id")
    .pivot("term_group", ["other", "group_a"])
    .agg(F.min("greatest_score_student_gpa"))
    .orderBy("student_id")
)
