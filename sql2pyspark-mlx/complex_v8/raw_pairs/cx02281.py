from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") <= 73.7) & (F.col("grade_status").isin("PASS", "INCOMPLETE", "FAIL")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

with_parent = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (~F.col("st.is_international")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

grouped = (
    with_parent
    .groupBy("student_id")
    .agg(
        F.array_sort(F.collect_set("student_cohort")).alias("student_cohort_list"),
        F.count("*").alias("n_rows"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
    )
)

result = (
    grouped
    .select("student_id", "n_rows", "distinct_student_ids")
    .unpivot(
        ids=["student_id"],
        values=["n_rows", "distinct_student_ids"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
