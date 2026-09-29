from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("term") == "2024T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

with_details = (
    scoped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.major").alias("student_major"),
    )
)

with_details_course = (
    with_details.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (~F.col("c.dept").isin("Bio", "Biz")), "inner")
    .filter(F.col("c.dept").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    with_details_course
    .groupingSets(
        [["course_id", "student_id"], ["course_id"], ["student_id"], []],
        "course_id", "student_id",
    )
    .agg(
        F.sum(F.when(F.col("grade_status") == "FAIL", 1).otherwise(0)).alias("n_grade_status_fail"),
        F.count("*").alias("cnt"),
    )
)
