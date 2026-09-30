from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") != "INCOMPLETE") & (F.col("score") < 73.7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

exploded_detail_enrollments = (
    prepared_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_grade_status_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
    )
)

with_details_course = (
    exploded_detail_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", F.col("c.credits").alias("course_credits"))
)

result = (
    with_details_course
    .select(
        "id",
        "enrollment_id",
        F.col("enrollment_score").alias("m_enrollment_score"),
        F.col("score").alias("m_score"),
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["m_enrollment_score", "m_score"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
