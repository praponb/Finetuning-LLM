from pyspark.sql import functions as F

base = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
        F.substring(F.trim(F.col("level")), 1, 4).alias("level_prefix"),
    )
)

dept_stats = (
    base
    .groupBy("dept")
    .agg(F.percentile_approx("credits", 0.5).alias("median_credits"))
)

compared = (
    base.alias("s")
    .join(dept_stats.alias("g"), F.col("g.dept") == F.col("s.dept"), "inner")
    .filter(F.col("s.credits") >= F.col("g.median_credits"))
    .select(
        "s.*",
        "g.median_credits",
        F.round(F.col("s.credits") - F.col("g.median_credits"), 2).alias("credits_minus_median"),
    )
)

summary = (
    compared
    .filter(F.col("credits_minus_median") > -2.4)
    .groupBy("credits_filled", "dept")
    .agg(
        F.max("credits_filled").alias("max_credits_filled"),
        F.min("credits_minus_median").alias("min_credits_minus_median"),
        F.sum("credits").alias("total_credits"),
        F.array_sort(F.collect_set("level")).alias("level_list"),
        F.countDistinct("dept").alias("distinct_depts"),
        F.min("credits").alias("min_credits"),
        F.avg("credits_filled").alias("avg_credits_filled_check"),
    )
    .filter(F.col("avg_credits_filled_check") > 2)
    .drop("avg_credits_filled_check")
)

result = (
    summary
    .select(
        "credits_filled",
        "dept",
        "min_credits_minus_median",
        "total_credits",
        "level_list",
        "min_credits",
        F.abs(F.col("credits_filled") - 3).alias("credits_filled_distance"),
        F.round(F.col("credits_filled") / F.nullif(F.col("total_credits"), F.lit(0)), 3).alias("credits_filled_per_total_credits"),
    )
    .orderBy(F.col("min_credits_minus_median").desc(), "credits_filled", "dept")
    .limit(20)
)
