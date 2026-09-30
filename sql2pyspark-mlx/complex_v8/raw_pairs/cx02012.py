from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2024T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.abs(F.col("score") - 44.4).alias("score_distance"),
    )
)

with_parent = (
    valid_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_term_label",
        "s.days_since_enrolled_at",
        "s.score_distance",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["student_id", "course_id", "grade_status"], ["student_id"], ["course_id"]],
        "student_id", "course_id", "grade_status",
    )
    .agg(F.round(F.stddev("course_credits"), 2).alias("stddev_course_credits"))
    .select(
        "student_id",
        "course_id",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "stddev_course_credits",
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
