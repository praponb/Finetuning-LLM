from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter(F.col("score") > 17.4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.when(F.col("term").isin("2025T2", "2024T1"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
    )
)

stats = (
    base_enrollments
    .groupBy("term_group", "grade_status_len", "student_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("term_group").alias("distinct_term_groups"),
    )
    .filter(F.col("row_count") >= 2)
)

with_details_student = (
    stats.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.term_group",
        "s.grade_status_len",
        "s.student_id",
        "s.row_count",
        "s.distinct_term_groups",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    with_details_student
    .groupBy("student_id", "distinct_term_groups")
    .pivot("student_cohort", ["C2023", "C2024", "C2021"])
    .agg(F.sum("grade_status_len"))
    .orderBy("student_id", "distinct_term_groups")
)
