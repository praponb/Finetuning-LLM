from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("score") > 42.4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

derived = (
    recent
    .select(
        "*",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
        F.abs(F.col("score") - 38.3).alias("score_distance"),
        F.abs(F.col("score") - 55.3).alias("score_distance_2"),
    )
)

combined_details_course = (
    derived.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.grade_status_ref",
        "s.score_distance",
        "s.score_distance_2",
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    combined_details_course
    .select("id", "score_distance", "score_distance_2")
    .unpivot(
        ids=["id"],
        values=["score_distance", "score_distance_2"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
