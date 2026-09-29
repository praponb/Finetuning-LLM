from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter((~F.col("grade_status").isin("WITHDRAWN", "FAIL")) & (F.col("term") != "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 55.6).alias("score_distance"),
    )
)

combined_details_student = (
    cleaned.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_distance",
        F.col("st.major").alias("student_major"),
        "c.course_code",
    )
)

term_stats = (
    combined_details_student
    .groupBy("term")
    .agg(F.avg("score_distance").alias("avg_score_distance"))
)

above_average = (
    combined_details_student.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score_distance") > F.col("g.avg_score_distance"))
    .select(
        "s.*",
        "g.avg_score_distance",
        F.round(F.col("s.score_distance") - F.col("g.avg_score_distance"), 2).alias("score_distance_minus_avg"),
    )
)

without_matches = (
    above_average.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

result = (
    without_matches
    .groupingSets(
        [["term", "student_id"], ["term"], []],
        "term", "student_id",
    )
    .agg(
        F.grouping("term").alias("is_total_term"),
        F.round(F.sum("avg_score_distance") / F.nullif(F.sum("score_distance"), F.lit(0)), 3).alias("avg_score_distance_to_score_distance_ratio"),
        F.round(F.sum("score_distance_minus_avg"), 2).alias("total_score_distance_minus_avg"),
    )
    .orderBy(F.col("term").asc_nulls_last(), F.col("student_id").asc_nulls_last())
)
