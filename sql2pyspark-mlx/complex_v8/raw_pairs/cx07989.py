from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter((F.col("student_id") <= 7) | (F.col("term").isin("2024T2", "2025T2")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

enriched_student = (
    valid.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.grade_status_clean",
        "s.enrolled_at_week",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
    )
)

joined_course = (
    enriched_student.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") > 2), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_clean",
        "s.student_gpa",
        "s.student_enrolled_on",
        "s.student_name",
        "c.course_code",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(~F.col("term").isin("2025T2", "2025T1"), 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
    )
)

profiled = (
    joined_course.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

result = (
    profiled
    .cube("enrollment_term_count", "enrollment_count_2")
    .agg(
        F.grouping("enrollment_term_count").alias("is_total_enrollment_term_count"),
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("course_code"))) >= 4, 1)) / F.count("*"), 1).alias("pct_length_trim_course_code_4"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
    )
)
