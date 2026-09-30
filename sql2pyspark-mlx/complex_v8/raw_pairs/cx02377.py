from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter(~F.col("term").isin("2025T1", "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

course_id_stats = (
    base_enrollments
    .groupBy("course_id")
    .agg(F.max("score").alias("max_score"))
)

above_average = (
    base_enrollments.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") == F.col("g.max_score"))
    .select(
        "s.*",
        "g.max_score",
        F.round(F.col("s.score") - F.col("g.max_score"), 2).alias("score_minus_max"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.round(F.avg("score"), 2).alias("avg_enrollment_score"))
)

with_stats = (
    above_average.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.avg_enrollment_score")
)

qualified = (
    with_stats.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.gpa") < 3.2) & (F.year(F.col("st.enrolled_on")) == 2025), "left_semi")
)

result = (
    qualified
    .rollup("student_id", "term", "grade_status")
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.max("score").alias("max_score_2"),
        F.sum("score").alias("total_score"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
