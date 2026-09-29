from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("course_id") >= 7) & (F.col("score") > 8.7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

with_parent_student = (
    cleaned_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_slug",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
        "st.student_name",
    )
)

without_matches = (
    with_parent_student.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score").between(8.8, 76.8)), "left_anti")
)

exploded_detail_enrollments = (
    without_matches.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status") == "WITHDRAWN"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.student_is_international",
        "s.student_major",
        "s.student_name",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

combined_details = (
    exploded_detail_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.student_is_international",
        "s.student_major",
        "s.student_name",
        "s.enrollment_id",
        "s.enrollment_course_id",
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

with_parent = (
    combined_details.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.student_is_international",
        "s.student_major",
        "s.student_name",
        "s.enrollment_id",
        "s.enrollment_course_id",
        "s.enrollment_enrolled_at",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

enrollment_course_id_stats = (
    with_parent
    .groupBy("enrollment_course_id")
    .agg(F.avg("course_credits").alias("avg_course_credits"))
)

above_average = (
    with_parent.alias("s")
    .join(enrollment_course_id_stats.alias("g"), F.col("g.enrollment_course_id") == F.col("s.enrollment_course_id"), "inner")
    .filter(F.col("s.course_credits") > F.col("g.avg_course_credits"))
    .select(
        "s.*",
        "g.avg_course_credits",
        F.round(F.col("s.course_credits") - F.col("g.avg_course_credits"), 2).alias("course_credits_minus_avg"),
    )
)

result = (
    above_average
    .filter(F.col("enrollment_course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "student_major",
        "student_name",
        "enrollment_id",
        "enrollment_course_id",
        "enrollment_enrolled_at",
        "course_level",
        "course_credits",
        "avg_course_credits",
        "course_credits_minus_avg",
    )
    .orderBy(
        F.col("course_level").asc(),
        F.col("course_credits_minus_avg").asc_nulls_last(),
    )
)
