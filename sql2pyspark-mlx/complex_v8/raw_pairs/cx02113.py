from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 10)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.abs(F.col("score") - 32.7).alias("score_distance"),
    )
)

grade_status_stats = (
    recent_enrollments
    .groupBy("grade_status")
    .agg(F.avg("term_len").alias("avg_term_len"))
)

compared = (
    recent_enrollments.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.term_len") > F.col("g.avg_term_len"))
    .select(
        "s.*",
        "g.avg_term_len",
        F.round(F.col("s.term_len") - F.col("g.avg_term_len"), 2).alias("term_len_minus_avg"),
    )
)

joined_student = (
    compared.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .filter(F.col("st.enrolled_on") < F.lit("2025-08-27").cast("date"))
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    joined_student
    .groupingSets(
        [["student_major", "student_is_international", "term"], ["student_major"]],
        "student_major", "student_is_international", "term",
    )
    .agg(
        F.max("term_len").alias("max_term_len"),
        F.array_sort(F.collect_set("student_major")).alias("student_major_list"),
    )
    .select(
        F.coalesce(F.col("student_major"), F.lit("ALL")).alias("student_major"),
        "student_is_international",
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "max_term_len",
        "student_major_list",
    )
)
