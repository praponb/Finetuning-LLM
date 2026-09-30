from pyspark.sql import functions as F

base_courses = (
    df_courses
    .filter((F.col("credits") < 3) | (F.upper(F.trim(F.col("course_code"))).like("M%")))
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.concat(F.col("course_code"), F.lit("#"), F.col("credits").cast("string")).alias("course_code_ref"),
        F.coalesce(F.col("dept"), F.lit("UNKNOWN")).alias("dept_or_unknown"),
        F.when(F.col("dept").isin("Math", "Bio"), "group_a").when(F.col("dept") == "Biz", "group_b").otherwise("other").alias("dept_group"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    base_courses.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.score") <= 78.8), "left_anti")
    .filter(F.col("level").isNotNull())
)

per_group = (
    with_activity
    .groupBy("enrollment_count", "dept_group", "dept")
    .agg(
        F.max("min_enrollment_score").alias("max_min_enrollment_score"),
        F.count("*").alias("n_rows"),
        F.countDistinct("dept_group").alias("distinct_dept_groups"),
        F.sum("min_enrollment_score").alias("total_min_enrollment_score"),
    )
)

labeled = (
    per_group
    .filter(F.col("dept_group") == "other")
    .select(
        "*",
        F.abs(F.col("enrollment_count") - 4).alias("enrollment_count_distance"),
        F.length(F.trim(F.col("dept_group"))).alias("dept_group_len"),
    )
)

annotated = (
    labeled
    .filter(F.col("n_rows") < 11)
    .select(
        "*",
        F.greatest(F.col("dept_group_len"), F.col("n_rows")).alias("greatest_dept_group_len_n_rows"),
        F.coalesce(F.col("dept"), F.lit("UNKNOWN")).alias("dept_or_unknown"),
        F.when(F.col("dept_group_len") >= 11, "large").when(F.col("dept_group_len") >= 7, "medium").otherwise("small").alias("dept_group_len_tier"),
    )
)

dept_group_len_tier_stats = (
    annotated
    .groupBy("dept_group_len_tier")
    .agg(F.avg("n_rows").alias("avg_n_rows"))
)

compared = (
    annotated.alias("s")
    .join(dept_group_len_tier_stats.alias("g"), F.col("g.dept_group_len_tier") == F.col("s.dept_group_len_tier"), "inner")
    .filter(F.col("s.n_rows") > F.col("g.avg_n_rows"))
    .select(
        "s.*",
        "g.avg_n_rows",
        F.round(F.col("s.n_rows") - F.col("g.avg_n_rows"), 2).alias("n_rows_minus_avg"),
    )
)

result = (
    compared
    .select(
        "enrollment_count",
        "dept_group",
        "max_min_enrollment_score",
        "n_rows",
        "total_min_enrollment_score",
        "greatest_dept_group_len_n_rows",
        "dept_or_unknown",
        "dept_group_len_tier",
        "avg_n_rows",
        "n_rows_minus_avg",
        F.coalesce(F.col("dept_group"), F.lit("UNKNOWN")).alias("dept_group_or_unknown"),
    )
    .orderBy(
        F.col("greatest_dept_group_len_n_rows").desc(),
        F.col("enrollment_count").desc(),
    )
)
