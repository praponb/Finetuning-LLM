from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("term").isNotNull()) & (F.col("grade_status") == "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

without_matches = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.month(F.col("e.enrolled_at")).isin(1, 2)), "left_anti")
)

totals = (
    without_matches
    .groupBy("student_id", "term", "grade_status_len")
    .agg(
        F.sum("grade_status_len").alias("total_grade_status_len"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

enriched_student = (
    totals.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.is_international")), "left")
    .select(
        "s.student_id",
        "s.term",
        "s.grade_status_len",
        "s.total_grade_status_len",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

result = (
    enriched_student
    .select(
        "student_id",
        "grade_status_len",
        "student_is_international",
        "student_gpa",
        F.abs(F.col("total_grade_status_len") - 39).alias("total_grade_status_len_distance"),
    )
    .orderBy(F.col("total_grade_status_len_distance").desc())
)
