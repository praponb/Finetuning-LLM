from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

course_id_stats = (
    cleaned
    .groupBy("course_id")
    .agg(F.avg("score").alias("avg_score"))
)

above_average = (
    cleaned.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

screened = (
    above_average.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id").isNotNull()), "left_anti")
    .filter(F.col("score_minus_avg") <= 60.0)
)

enriched = (
    screened.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        "st.student_name",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    enriched
    .groupBy("course_id")
    .pivot("grade_status", ["INCOMPLETE", "FAIL", "PASS", "WITHDRAWN"])
    .agg(F.max("score_adj"))
)
