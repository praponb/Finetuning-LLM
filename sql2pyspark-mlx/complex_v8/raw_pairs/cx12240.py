from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("score") <= 59.7) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

enriched = (
    scoped_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
        "c.course_code",
    )
)

enriched_course = (
    enriched.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") <= 3), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.student_enrolled_on",
        "s.student_name",
        "s.course_code",
        F.col("c.course_code").alias("course_course_code"),
    )
)

result = (
    enriched_course
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "student_enrolled_on",
        "student_name",
        "course_code",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("course_code")).alias("grade_status_course_code_label"),
    )
    .orderBy("course_id")
)
