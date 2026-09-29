from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") > 36.6) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

detail_rows_enrollments = (
    filtered_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.grade_status_or_unknown",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
    )
)

labeled = (
    detail_rows_enrollments
    .select(
        "*",
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        F.date_trunc("hour", F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

detail_rows = (
    labeled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status_or_unknown",
        "s.enrollment_id",
        "s.enrollment_student_id",
        "s.enrollment_term",
        "s.term_ref",
        "s.enrolled_at_hour",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.score").alias("enrollment_score_2"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id_2"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

scored = detail_rows.select("*", F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"))

with_children_enrollments = (
    scored.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") < 74.9), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_or_unknown",
        "s.enrollment_id",
        "s.enrollment_student_id",
        "s.enrolled_at_hour",
        "s.enrollment_id_2",
        "s.enrollment_score_2",
        "s.enrollment_grade_status",
        "s.enrollment_student_id_2",
        "s.score_filled",
        F.col("e.id").alias("enrollment_id_3"),
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.student_id").alias("enrollment_student_id_3"),
    )
)

joined_course = (
    with_children_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_or_unknown",
        "s.enrollment_id",
        "s.enrollment_student_id",
        "s.enrollment_id_2",
        "s.enrollment_score_2",
        "s.enrollment_student_id_2",
        "s.enrollment_id_3",
        "s.enrollment_enrolled_at_2",
        "s.enrollment_score",
        "s.enrollment_student_id_3",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
    )
)

result = (
    joined_course
    .select(
        "id",
        "enrollment_id",
        "enrollment_score",
        "enrollment_score_2",
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["enrollment_score", "enrollment_score_2"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "enrollment_id", "metric")
)
