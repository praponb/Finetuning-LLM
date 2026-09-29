from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

tagged = (
    recent_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

summary = (
    tagged
    .groupBy("term", "student_id")
    .agg(
        F.sum("score").alias("total_score"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
    )
)

result = (
    summary
    .select(
        "term",
        "student_id",
        "stddev_score",
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
    )
)
