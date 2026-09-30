from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_running = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_term_by_enrolled = Window.partitionBy("student_id", "term").orderBy("enrolled_at", "id")
w_grade_status_or_unknown_2_co = Window.partitionBy("grade_status_or_unknown_2", "course_id").orderBy(F.col("score").desc(), "id")
w_grade_status_or_unknown_2_sc = Window.partitionBy("grade_status_or_unknown_2", "score_tier")
w_grade_status_or_unknown_grad = Window.partitionBy("grade_status_or_unknown", "grade_status_or_unknown_2")
w_all_by_enrolled_at_rank_full = Window.orderBy("enrolled_at_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_enrolled_at_rank = Window.orderBy("enrolled_at_rank", "id")

valid = (
    df_enrollments
    .filter((F.col("grade_status") == "WITHDRAWN") & (F.hour(F.col("enrolled_at")).between(10, 15)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 78.8, "HIGH").when(F.col("score") >= 11.1, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
    )
)

score_tier_stats = (
    valid
    .groupBy("score_tier")
    .agg(F.avg("score").alias("avg_score"))
)

compared = (
    valid.alias("s")
    .join(score_tier_stats.alias("g"), F.col("g.score_tier") == F.col("s.score_tier"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

annotated = (
    compared
    .select(
        "*",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("score_tier")), " ", "_")).alias("score_tier_slug"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown_2"),
    )
)

windowed = (
    annotated
    .select(
        "id",
        "student_id",
        "score",
        "score_tier",
        "enrolled_at_label",
        "grade_status_ref",
        "avg_score",
        "grade_status_or_unknown",
        "score_tier_slug",
        "grade_status_or_unknown_2",
        F.first("enrolled_at", ignorenulls=True).over(w_all_by_score_running).alias("first_known_enrolled_at"),
        F.rank().over(w_student_id_term_by_enrolled).alias("enrolled_at_rank"),
        F.round(F.cume_dist().over(w_student_id_term_by_enrolled), 3).alias("enrolled_at_cume_dist"),
        F.ntile(5).over(w_grade_status_or_unknown_2_co).alias("score_quintile"),
        F.round(F.col("avg_score") - F.avg("avg_score").over(w_grade_status_or_unknown_2_sc), 2).alias("avg_score_vs_grade_status_or_unknown_2_score_tie"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_stats = (
    windowed.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

windowed_2 = (
    with_stats
    .select(
        "*",
        F.sum("avg_score").over(w_grade_status_or_unknown_grad).alias("grade_status_or_unknown_grade_status_or_unknown_"),
        F.last("avg_score").over(w_all_by_enrolled_at_rank_full).alias("last_avg_score"),
        F.rank().over(w_all_by_enrolled_at_rank).alias("enrolled_at_rank_rank"),
    )
)

score_tier_stats_2 = (
    windowed_2
    .groupBy("score_tier")
    .agg(F.avg("enrolled_at_rank_rank").alias("avg_enrolled_at_rank_rank"))
)

outliers = (
    windowed_2.alias("s")
    .join(score_tier_stats_2.alias("g"), F.col("g.score_tier") == F.col("s.score_tier"), "inner")
    .filter(F.col("s.enrolled_at_rank_rank") > F.col("g.avg_enrolled_at_rank_rank"))
    .select(
        "s.*",
        "g.avg_enrolled_at_rank_rank",
        F.round(F.col("s.enrolled_at_rank_rank") - F.col("g.avg_enrolled_at_rank_rank"), 2).alias("enrolled_at_rank_rank_minus_avg"),
    )
)

result = (
    outliers
    .filter(F.col("enrolled_at_rank_rank") > 5)
    .select(
        "id",
        "student_id",
        "score",
        "score_tier",
        "enrolled_at_label",
        "grade_status_ref",
        "avg_score",
        "grade_status_or_unknown",
        "score_tier_slug",
        "grade_status_or_unknown_2",
        "enrolled_at_rank",
        "score_quintile",
        "avg_score_vs_grade_status_or_unknown_2_score_tie",
        "enrollment_count",
        "last_avg_score",
        "enrolled_at_rank_rank",
        "avg_enrolled_at_rank_rank",
        "enrolled_at_rank_rank_minus_avg",
        F.lower(F.regexp_replace(F.trim(F.col("score_tier")), " ", "_")).alias("score_tier_slug_2"),
    )
    .orderBy(F.col("id").desc(), F.col("avg_score").asc_nulls_last())
)
