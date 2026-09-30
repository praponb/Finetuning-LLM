from pyspark.sql import functions as F

prepared = df_enrollments.filter(F.col("student_id") <= 8)

course_id_stats = (
    prepared
    .groupBy("course_id")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

outliers = (
    prepared.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

student_id_stats = (
    outliers
    .groupBy("student_id")
    .agg(F.max("score").alias("max_score"))
)

above_average = (
    outliers.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") == F.col("g.max_score"))
    .select(
        "s.*",
        "g.max_score",
        F.round(F.col("s.score") - F.col("g.max_score"), 2).alias("score_minus_max"),
    )
)

line_level = (
    above_average.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.median_score",
        "s.max_score",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

result = (
    line_level
    .select(
        "id",
        "student_id",
        "score",
        "median_score",
        "enrollment_id",
        "enrollment_course_id",
        (F.floor(F.col("max_score") / 25) * 25).alias("max_score_bucket"),
    )
    .orderBy("max_score_bucket")
)
