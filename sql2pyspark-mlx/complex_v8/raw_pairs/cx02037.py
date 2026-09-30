from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.col("student_id") >= 11) & (F.col("course_id") >= 6))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

screened = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term").isNotNull()), "left_anti")
    .filter(F.col("grade_status").isNotNull())
)

per_group = (
    screened
    .groupBy(F.to_date("enrolled_at").alias("enrolled_at_day"))
    .agg(
        F.min("score").alias("min_score"),
        F.percentile_approx("score", 0.5).alias("median_score"),
        F.sum("score").alias("total_score"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
    )
)

screened_2 = (
    per_group
    .crossJoin(per_group.agg(F.min("total_score").alias("overall_min_total_score")))
    .filter(F.col("total_score") > F.col("overall_min_total_score"))
    .drop("overall_min_total_score")
)

tagged = (
    screened_2
    .filter(F.col("stddev_score") >= 40.0)
    .select(
        "*",
        F.date_trunc("quarter", F.col("enrolled_at_day")).alias("enrolled_at_day_quarter"),
        F.round(F.col("stddev_score") * 1.1, 2).alias("stddev_score_adj"),
        F.date_add(F.col("enrolled_at_day"), 7).alias("enrolled_at_day_plus_7d"),
    )
)

screened_3 = (
    tagged
    .crossJoin(tagged.agg(F.avg("total_score").alias("overall_avg_total_score")))
    .filter(F.col("total_score") > F.col("overall_avg_total_score"))
    .drop("overall_avg_total_score")
)

screened_4 = (
    screened_3
    .crossJoin(screened_3.agg(F.avg("stddev_score_adj").alias("overall_avg_stddev_score_adj")))
    .filter(F.col("stddev_score_adj") > F.col("overall_avg_stddev_score_adj"))
    .drop("overall_avg_stddev_score_adj")
)

result = (
    screened_4
    .filter(F.year(F.col("enrolled_at_day_plus_7d")) == 2025)
    .select(
        "enrolled_at_day",
        "min_score",
        "median_score",
        "total_score",
        "stddev_score",
        "enrolled_at_day_quarter",
        "enrolled_at_day_plus_7d",
        F.round(F.col("stddev_score_adj") * 1.5, 2).alias("stddev_score_adj_adj"),
    )
    .orderBy("stddev_score")
)
