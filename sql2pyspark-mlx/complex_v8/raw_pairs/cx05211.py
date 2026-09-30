from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("grade_status") != "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

labeled = (
    valid
    .select(
        "*",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.round(F.col("score") / F.nullif(F.col("score_filled"), F.lit(0)), 3).alias("score_per_score_filled"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

with_parent = (
    labeled.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_filled",
        "s.enrolled_at_label",
        "s.score_per_score_filled",
        "s.term_prefix",
        F.col("c.level").alias("course_level"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["grade_status", "course_level"], ["grade_status"], ["course_level"]],
        "grade_status", "course_level",
    )
    .agg(
        F.grouping("grade_status").alias("is_total_grade_status"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        F.coalesce(F.col("course_level"), F.lit("ALL")).alias("course_level"),
        "is_total_grade_status",
        "cnt",
    )
)
