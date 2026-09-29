from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter(F.col("score") < 62.5)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

combined_details_student = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_slug",
        "s.grade_status_prefix",
        F.col("st.gpa").alias("student_gpa"),
        "st.student_name",
        F.col("st.is_international").alias("student_is_international"),
        F.col("c.level").alias("course_level"),
    )
)

without_matches = (
    combined_details_student.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") < 62.5), "left_anti")
)

labeled = (
    without_matches
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at",
        "grade_status_slug",
        "grade_status_prefix",
        "student_gpa",
        "student_name",
        "student_is_international",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
)

enriched = (
    labeled
    .withColumn("enrolled_at_label_prefix", F.substring(F.trim(F.col("enrolled_at_label")), 1, 2))
)

line_level = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status_prefix",
        "s.student_gpa",
        "s.student_name",
        "s.student_is_international",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

enriched_2 = (
    line_level.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.student_name",
        "s.student_is_international",
        "s.enrollment_id",
        "s.enrollment_grade_status",
        "s.enrollment_enrolled_at",
        "s.enrollment_student_id",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "student_name",
        "student_is_international",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_enrolled_at",
        "course_credits",
        "course_dept",
        "course_level",
    )
    .orderBy(F.col("course_credits").desc_nulls_first(), "id", "enrollment_id")
    .limit(20)
)
