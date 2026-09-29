from pyspark.sql import functions as F

recent = (
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

with_parent_student = (
    recent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

student_cohort_stats = (
    with_parent_student
    .groupBy("student_cohort")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

above_average = (
    with_parent_student.alias("s")
    .join(student_cohort_stats.alias("g"), F.col("g.student_cohort") == F.col("s.student_cohort"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

result = (
    above_average
    .groupBy("student_enrolled_on", "term")
    .pivot("grade_status", ["FAIL", "PASS"])
    .agg(F.sum("score_minus_median").alias("amount"), F.max("score_minus_median").alias("max_v"))
    .orderBy("student_enrolled_on", "term")
)
