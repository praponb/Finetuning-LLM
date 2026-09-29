from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter((F.col("score") >= 11.3) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.round(F.avg("score"), 2).alias("avg_enrollment_score"))
)

profiled = (
    valid.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.avg_enrollment_score")
)

filtered = (
    profiled.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (~F.col("c.level").isin("core", "intro")) & (F.col("c.credits") >= 2), "left_semi")
)

qualified = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") <= 10), "left_anti")
)

scored = (
    qualified
    .filter(F.col("course_id") >= 5)
    .select(
        "*",
        (F.floor(F.col("avg_enrollment_score") / 25) * 25).alias("avg_enrollment_score_bucket"),
        F.concat_ws("|", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "term_prefix",
        "avg_enrollment_score",
        "term_grade_status_label",
        (F.floor(F.col("term_len") / 2) * 2).alias("term_len_bucket"),
        F.round(F.col("avg_enrollment_score") * 1.5, 2).alias("avg_enrollment_score_adj"),
    )
    .orderBy(F.col("id").desc())
)
