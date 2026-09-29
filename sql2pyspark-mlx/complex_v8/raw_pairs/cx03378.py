from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter(F.col("grade_status") == "INCOMPLETE")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

summary = (
    base
    .filter(F.col("score") > 8.7)
    .groupBy("course_id", "student_id", "term")
    .agg(
        F.max("score").alias("max_score"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.percentile_approx("score", 0.5).alias("median_score"),
        F.count(F.when(F.col("grade_status") != "WITHDRAWN", 1)).alias("n_grade_status_withdrawn"),
    )
    .filter(F.col("max_score") > 26.2)
)

combined_details = (
    summary.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.course_id",
        "s.student_id",
        "s.term",
        "s.max_score",
        "s.grade_status_list",
        "s.n_grade_status_withdrawn",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

filtered = (
    combined_details.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
    .filter(F.col("course_id") >= 3)
)

result = (
    filtered
    .select(
        "course_id",
        "student_id",
        "student_gpa",
        "max_score",
    )
    .unpivot(
        ids=["course_id", "student_id"],
        values=["student_gpa", "max_score"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
