from pyspark.sql import functions as F

prepared = (
    df_courses
    .filter(F.col("credits").between(2, 4))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("dept").isin("Biz", "CS"), "group_a").when(F.col("dept") == "Bio", "group_b").otherwise("other").alias("dept_group"),
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
    )
)

without_matches = (
    prepared
    .crossJoin(prepared.agg(F.max("dept_len").alias("overall_max_dept_len")))
    .filter(F.col("dept_len") < F.col("overall_max_dept_len"))
    .drop("overall_max_dept_len")
)

tagged = (
    without_matches
    .filter(~F.col("level").isin("intro", "core"))
    .withColumn("course_code_len", F.length(F.trim(F.col("course_code"))))
)

dept_stats = (
    tagged
    .groupBy("dept")
    .agg(F.avg("dept_len").alias("avg_dept_len"))
)

above_average = (
    tagged.alias("s")
    .join(dept_stats.alias("g"), F.col("g.dept") == F.col("s.dept"), "inner")
    .filter(F.col("s.dept_len") > F.col("g.avg_dept_len"))
    .select(
        "s.*",
        "g.avg_dept_len",
        F.round(F.col("s.dept_len") - F.col("g.avg_dept_len"), 2).alias("dept_len_minus_avg"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    above_average.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

annotated = (
    with_counts
    .select(
        "*",
        F.round(F.col("course_code_len") / F.nullif(F.col("avg_dept_len"), F.lit(0)), 3).alias("course_code_len_per_avg_dept_len"),
        F.abs(F.col("dept_len") - 7).alias("dept_len_distance"),
    )
)

with_activity = (
    annotated
    .crossJoin(annotated.agg(F.avg("total_enrollment_score").alias("overall_avg_total_enrollment_score")))
    .filter(F.col("total_enrollment_score") > F.col("overall_avg_total_enrollment_score"))
    .drop("overall_avg_total_enrollment_score")
)

result = (
    with_activity
    .filter(F.col("course_code_len_per_avg_dept_len") <= 6.0)
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        "dept_len",
        "dept_len_minus_avg",
        "total_enrollment_score",
        "enrollment_count",
        "dept_len_distance",
        (F.floor(F.col("dept_len") / 2) * 2).alias("dept_len_bucket"),
    )
    .orderBy(F.col("total_enrollment_score").asc())
)
