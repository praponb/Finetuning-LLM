from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

joined_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

annotated = (
    joined_course
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "course_level",
        "course_code",
        F.abs(F.col("score") - 57.6).alias("score_distance"),
    )
)

result = (
    annotated
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "score_distance",
        F.round(F.col("score_distance") / F.nullif(F.col("score"), F.lit(0)), 3).alias("score_distance_per_score"),
    )
    .orderBy("course_id")
)
