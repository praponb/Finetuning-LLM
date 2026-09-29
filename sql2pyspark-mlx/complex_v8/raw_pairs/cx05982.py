from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter(F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN", "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

with_parent_course = (
    base.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.grade_status_term_label",
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

result = (
    with_parent_course
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "grade_status_term_label",
        "course_code",
        "course_level",
    )
    .orderBy("id", F.col("grade_status").desc())
)
