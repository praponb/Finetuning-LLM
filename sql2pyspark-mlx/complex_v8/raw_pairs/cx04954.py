from pyspark.sql import functions as F

filtered = (
    df_courses
    .filter((F.col("credits") < 4) & (F.col("course_code").isNotNull()))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.substring(F.trim(F.col("dept")), 1, 3).alias("dept_prefix"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select("s.*", "e.avg_enrollment_score", "e.total_enrollment_score")
)

dept_stats = (
    with_stats
    .groupBy("dept")
    .agg(F.max("total_enrollment_score").alias("max_total_enrollment_score"))
)

outliers = (
    with_stats.alias("s")
    .join(dept_stats.alias("g"), F.col("g.dept") == F.col("s.dept"), "inner")
    .filter(F.col("s.total_enrollment_score") == F.col("g.max_total_enrollment_score"))
    .select(
        "s.*",
        "g.max_total_enrollment_score",
        F.round(F.col("s.total_enrollment_score") - F.col("g.max_total_enrollment_score"), 2).alias("total_enrollment_score_minus_max"),
    )
)

level_stats = (
    outliers
    .groupBy("level")
    .agg(F.max("total_enrollment_score").alias("max_total_enrollment_score_2"))
)

compared = (
    outliers.alias("s")
    .join(level_stats.alias("g"), F.col("g.level") == F.col("s.level"), "inner")
    .filter(F.col("s.total_enrollment_score") == F.col("g.max_total_enrollment_score_2"))
    .select(
        "s.*",
        "g.max_total_enrollment_score_2",
        F.round(F.col("s.total_enrollment_score") - F.col("g.max_total_enrollment_score_2"), 2).alias("total_enrollment_score_minus_max_2"),
    )
)

with_activity = (
    compared
    .crossJoin(compared.agg(F.avg("max_total_enrollment_score_2").alias("overall_avg_max_total_enrollment_score_2")))
    .filter(F.col("max_total_enrollment_score_2") > F.col("overall_avg_max_total_enrollment_score_2"))
    .drop("overall_avg_max_total_enrollment_score_2")
)

result = (
    with_activity
    .filter(F.col("max_total_enrollment_score") < 510.0)
    .select(
        "id",
        "dept",
        "credits",
        "level",
        "dept_prefix",
        "avg_enrollment_score",
        "total_enrollment_score",
        "max_total_enrollment_score",
        "total_enrollment_score_minus_max",
        "max_total_enrollment_score_2",
        "total_enrollment_score_minus_max_2",
        F.length(F.trim(F.col("level"))).alias("level_len"),
    )
)
