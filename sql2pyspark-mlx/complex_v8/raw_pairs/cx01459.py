from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
    )
)

with_parent_course = (
    prepared_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.score_adj",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    with_parent_course
    .groupingSets(
        [["term", "course_level"], ["term"], ["course_level"], []],
        "term", "course_level",
    )
    .agg(F.min("course_credits").alias("min_course_credits"))
    .select(
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        F.coalesce(F.col("course_level"), F.lit("ALL")).alias("course_level"),
        "min_course_credits",
    )
)
