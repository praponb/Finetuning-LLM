from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") != "INCOMPLETE") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

stats = (
    filtered_enrollments
    .groupBy("course_id", "grade_status")
    .agg(
        F.round(F.stddev("score_bucket"), 2).alias("stddev_score_bucket"),
        F.sum("score").alias("total_score"),
        F.min("score").alias("min_score"),
        F.round(F.avg("score_bucket"), 2).alias("avg_score_bucket"),
    )
)

combined_details = (
    stats.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(~F.col("c.level").isin("advanced", "intro"))
    .select(
        "s.course_id",
        "s.grade_status",
        "s.stddev_score_bucket",
        "s.total_score",
        "s.min_score",
        "s.avg_score_bucket",
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

derived = (
    combined_details
    .filter(F.col("course_id").isNotNull())
    .withColumn("grade_status_course_level_label", F.concat_ws(": ", F.col("grade_status"), F.col("course_level")))
)

course_level_stats = (
    derived
    .groupBy("course_level")
    .agg(F.avg("stddev_score_bucket").alias("avg_stddev_score_bucket"))
)

outliers = (
    derived.alias("s")
    .join(course_level_stats.alias("g"), F.col("g.course_level") == F.col("s.course_level"), "inner")
    .filter(F.col("s.stddev_score_bucket") > F.col("g.avg_stddev_score_bucket"))
    .select(
        "s.*",
        "g.avg_stddev_score_bucket",
        F.round(F.col("s.stddev_score_bucket") - F.col("g.avg_stddev_score_bucket"), 2).alias("stddev_score_bucket_minus_avg"),
    )
)

tagged = (
    outliers
    .select(
        "course_id",
        "grade_status",
        "stddev_score_bucket",
        "min_score",
        "avg_score_bucket",
        "course_code",
        "course_level",
        F.concat(F.col("course_code"), F.lit("#"), F.col("course_id").cast("string")).alias("course_code_ref"),
        F.concat_ws("-", F.col("grade_status"), F.col("course_level")).alias("grade_status_course_level_label_2"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

result = (
    tagged
    .select(
        "course_id",
        "grade_status",
        "stddev_score_bucket",
        "min_score",
        "avg_score_bucket",
        "course_code_ref",
        "grade_status_course_level_label_2",
        "grade_status_ref",
    )
    .orderBy(
        F.col("grade_status").desc(),
        F.col("stddev_score_bucket").desc(),
        "course_id",
    )
    .limit(5)
)
