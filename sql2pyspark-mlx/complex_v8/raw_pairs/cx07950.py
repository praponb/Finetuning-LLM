from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("student_id") <= 18) & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

combined_details_student = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.enrolled_at_quarter",
        "st.student_name",
    )
)

student_id_stats = (
    combined_details_student
    .groupBy("student_id")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

outliers = (
    combined_details_student.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

with_children = (
    outliers.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at_quarter",
        "s.student_name",
        "s.score_minus_median",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

with_details = (
    with_children.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.enrollment_student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at_quarter",
        "s.student_name",
        "s.score_minus_median",
        "s.enrollment_id",
        "s.enrollment_student_id",
        "s.enrollment_grade_status",
        "s.enrollment_score",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.student_name").alias("student_student_name"),
    )
)

result = (
    with_details
    .rollup("course_level", "enrollment_student_id", "enrollment_id")
    .agg(F.countDistinct("course_level").alias("distinct_course_levels"))
    .orderBy(
        F.col("course_level").asc_nulls_last(),
        F.col("enrollment_student_id").asc_nulls_last(),
        F.col("enrollment_id").asc_nulls_last(),
    )
)
