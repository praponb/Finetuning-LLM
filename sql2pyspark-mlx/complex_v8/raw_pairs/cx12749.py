from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        (F.col("enrolled_at") + F.expr("INTERVAL 2 HOURS")).alias("enrolled_at_plus_2h"),
    )
)

enriched_course = (
    prepared_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "c.course_code",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    enriched_course
    .rollup("course_id", "term")
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
    )
    .orderBy(F.col("course_id").asc_nulls_last(), F.col("term").asc_nulls_last())
)
