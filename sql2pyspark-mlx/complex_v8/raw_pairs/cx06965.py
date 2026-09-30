from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

with_details_course = (
    prepared_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.term_prefix",
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
        "st.student_name",
    )
)

derived = (
    with_details_course
    .select(
        "id",
        "student_id",
        "grade_status",
        "term_prefix",
        "course_code",
        "course_dept",
        "student_name",
        F.coalesce(F.col("course_dept"), F.lit("UNKNOWN")).alias("course_dept_or_unknown"),
    )
)

result = (
    derived
    .select(
        "id",
        "student_id",
        "term_prefix",
        "course_dept",
        "student_name",
        "course_dept_or_unknown",
        F.when(F.col("course_dept").isin("Bio", "CS"), "group_a").when(F.col("course_dept") == "Biz", "group_b").otherwise("other").alias("course_dept_group"),
    )
    .orderBy(F.col("student_id").desc())
)
