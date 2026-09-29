from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) | (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
    )
)

joined_course = (
    prepared.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") > 2), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_slug",
        "s.term_ref",
        "c.course_code",
        F.col("st.gpa").alias("student_gpa"),
    )
)

filtered = (
    joined_course.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") <= 5) & (F.month(F.col("e.enrolled_at")).isin(2, 3)), "left_anti")
)

qualified = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") == "2025T2"), "left_semi")
    .filter(F.month(F.col("enrolled_at")).isin(1, 2))
)

exploded_detail_enrollments = (
    qualified.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.grade_status_slug",
        "s.course_code",
        "s.student_gpa",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
    )
)

rollup_base = (
    exploded_detail_enrollments
    .groupBy(
        "enrollment_grade_status",
        F.when(F.col("score") >= 56.1, "large").when(F.col("score") >= 36.6, "medium").otherwise("small").alias("score_tier"),
        "enrollment_course_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.length(F.trim(F.col("course_code"))) >= 4, F.col("score")).otherwise(0)).alias("score_length_trim_course_code_4"),
        F.countDistinct("enrollment_course_id").alias("distinct_enrollment_course_ids"),
        F.array_sort(F.collect_set("enrollment_grade_status")).alias("enrollment_grade_status_list"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
    )
)

result = (
    rollup_base
    .select(
        "enrollment_grade_status",
        "score_tier",
        "enrollment_course_id",
        "cnt",
        "score_length_trim_course_code_4",
        "distinct_enrollment_course_ids",
        "enrollment_grade_status_list",
        "distinct_grade_status",
    )
    .orderBy(F.col("distinct_enrollment_course_ids").desc())
)
