from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("score") > 42.4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

with_parent = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

with_details_student = (
    with_parent.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.course_level",
        "s.course_code",
        "st.student_name",
        F.col("c.course_code").alias("course_course_code"),
        F.col("c.dept").alias("course_dept_2"),
    )
)

screened = (
    with_details_student.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
    .filter(F.col("course_dept_2").isNotNull())
)

scored = (
    screened
    .select(
        "*",
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
        F.when(F.col("score") >= 57.3, "A").when(F.col("score") >= 26.2, "B").otherwise("C").alias("score_tier"),
        F.when(F.col("score") >= 73.0, "A").when(F.col("score") >= 20.6, "B").otherwise("C").alias("score_tier_2"),
    )
)

course_level_stats = (
    scored
    .groupBy("course_level")
    .agg(F.percentile_approx("course_code_len", 0.5).alias("median_course_code_len"))
)

above_average = (
    scored.alias("s")
    .join(course_level_stats.alias("g"), F.col("g.course_level") == F.col("s.course_level"), "inner")
    .filter(F.col("s.course_code_len") >= F.col("g.median_course_code_len"))
    .select(
        "s.*",
        "g.median_course_code_len",
        F.round(F.col("s.course_code_len") - F.col("g.median_course_code_len"), 2).alias("course_code_len_minus_median"),
    )
)

result = (
    above_average
    .filter(F.col("score_tier_2") != "B")
    .select(
        "id",
        "course_id",
        "grade_status",
        "course_code",
        "student_name",
        "course_dept_2",
        "score_tier",
        "score_tier_2",
        "median_course_code_len",
    )
    .orderBy("grade_status")
)
