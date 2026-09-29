from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_term_is_not_null = Window.partitionBy("n_term_is_not_null")
w_term = Window.partitionBy("term")

scoped = (
    df_enrollments
    .filter(F.col("course_id") >= 10)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

summary = (
    scoped
    .filter(F.hour(F.col("enrolled_at")).between(7, 11))
    .groupBy("term", F.year("enrolled_at").alias("enrolled_at_year"))
    .agg(
        F.round(100 * F.count(F.when(F.col("grade_status") == "INCOMPLETE", 1)) / F.count("*"), 1).alias("pct_grade_status_incomplete"),
        F.sum(F.when(F.col("term").isNotNull(), 1).otherwise(0)).alias("n_term_is_not_null"),
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("n_term_is_not_null_2"),
    )
)

tagged = (
    summary
    .filter(F.col("enrolled_at_year") <= 2026)
    .select(
        "term",
        "enrolled_at_year",
        "n_term_is_not_null",
        (F.col("n_term_is_not_null_2") + F.col("n_term_is_not_null")).alias("n_term_is_not_null_2_plus_n_term_is_not_null"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
)

qualified = (
    tagged
    .crossJoin(tagged.agg(F.avg("n_term_is_not_null_2_plus_n_term_is_not_null").alias("overall_avg_n_term_is_not_null_2_plus_n_term_is_")))
    .filter(F.col("n_term_is_not_null_2_plus_n_term_is_not_null") > F.col("overall_avg_n_term_is_not_null_2_plus_n_term_is_"))
    .drop("overall_avg_n_term_is_not_null_2_plus_n_term_is_")
)

ranked = (
    qualified
    .select(
        "*",
        F.size(F.collect_set("term").over(w_n_term_is_not_null)).alias("distinct_term_in_n_term_is_not_null"),
        F.round(F.avg("n_term_is_not_null").over(w_n_term_is_not_null), 2).alias("n_term_is_not_null_avg_n_term_is_not_null"),
    )
)

with_trends = (
    ranked
    .select(
        "term",
        "enrolled_at_year",
        "n_term_is_not_null_2_plus_n_term_is_not_null",
        "distinct_term_in_n_term_is_not_null",
        "n_term_is_not_null_avg_n_term_is_not_null",
        F.sum("enrolled_at_year").over(w_term).alias("term_sum_enrolled_at_year"),
        F.min("enrolled_at_year").over(w_term).alias("term_min_enrolled_at_year"),
    )
)

enriched = (
    with_trends
    .filter(F.col("term") != "2025T2")
    .select(
        "term",
        "enrolled_at_year",
        "n_term_is_not_null_2_plus_n_term_is_not_null",
        "distinct_term_in_n_term_is_not_null",
        "n_term_is_not_null_avg_n_term_is_not_null",
        "term_sum_enrolled_at_year",
        "term_min_enrolled_at_year",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

result = (
    enriched
    .select(
        "term",
        "enrolled_at_year",
        "distinct_term_in_n_term_is_not_null",
        "term_sum_enrolled_at_year",
        "term_min_enrolled_at_year",
    )
    .orderBy("enrolled_at_year", F.col("term_sum_enrolled_at_year").desc())
)
