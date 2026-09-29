from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_score = Window.orderBy(F.col("total_score").desc(), "term_or_unknown")

valid = (
    df_enrollments
    .filter((F.col("score") <= 90.9) | (F.hour(F.col("enrolled_at")).between(8, 14)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.when(F.col("score") >= 62.5, "HIGH").when(F.col("score") >= 35.8, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

grouped = (
    valid
    .groupBy("term_or_unknown")
    .agg(
        F.sum(F.when(F.col("grade_status") != "WITHDRAWN", 1).otherwise(0)).alias("n_grade_status_withdrawn"),
        F.count("*").alias("cnt"),
        F.round(F.sum("score_filled") / F.nullif(F.sum("score"), F.lit(0)), 3).alias("score_filled_to_score_ratio"),
        F.round(F.sum("score"), 2).alias("total_score"),
        F.percentile_approx("score", 0.5).alias("median_score"),
        F.sum("score_filled").alias("sum_score_filled_check"),
    )
    .filter(F.col("sum_score_filled_check") > 30.0)
    .drop("sum_score_filled_check")
)

ranked = grouped.withColumn("seq_no", F.row_number().over(w_all_by_total_score))

screened = (
    ranked
    .crossJoin(ranked.agg(F.min("median_score").alias("overall_min_median_score")))
    .filter(F.col("median_score") > F.col("overall_min_median_score"))
    .drop("overall_min_median_score")
)

result = (
    screened
    .select(
        "term_or_unknown",
        "n_grade_status_withdrawn",
        "cnt",
        "total_score",
        "median_score",
    )
    .orderBy("cnt", F.col("median_score").asc_nulls_last())
)
