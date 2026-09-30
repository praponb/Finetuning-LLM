from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
    )
)

with_children_enrollments = (
    scoped_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (~F.col("e.grade_status").isin("FAIL", "WITHDRAWN")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

enriched_course = (
    with_children_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.lower(F.col("c.course_code")).like("%cs%")), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrollment_id",
        "s.enrollment_grade_status",
        "s.enrollment_course_id",
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
        F.col("c.level").alias("course_level"),
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
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_course_id",
        "course_dept",
        "course_code",
        "course_level",
    )
    .orderBy("course_dept", "enrollment_grade_status")
)
