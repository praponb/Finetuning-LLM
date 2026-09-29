from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("student_id") <= 14) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.abs(F.col("score") - 30.0).alias("score_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

combined_details = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.term_or_unknown",
        "s.score_distance",
        "s.days_since_enrolled_at",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.dept").alias("course_dept"),
    )
)

summary = (
    combined_details
    .filter(F.col("student_id") >= 17)
    .groupBy("student_id", "course_id", "course_dept")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("student_major").alias("distinct_student_majors"),
        F.round(F.avg("days_since_enrolled_at"), 2).alias("avg_days_since_enrolled_at"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.round(F.sum("days_since_enrolled_at") / F.nullif(F.sum("score_distance"), F.lit(0)), 3).alias("days_since_enrolled_at_to_score_distance_ratio"),
        F.min("student_enrolled_on").alias("first_student_enrolled_on"),
    )
)

result = (
    summary
    .groupBy("n_rows")
    .pivot("course_dept", ["Biz", "CS", "Art"])
    .agg(F.count("distinct_student_majors"))
)
