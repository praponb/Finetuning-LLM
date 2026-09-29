from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status = Window.partitionBy("grade_status")

prepared = (
    df_enrollments
    .filter(F.col("course_id") <= 5)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2025T1", "2025T2"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
        F.when(F.col("score") >= 57.6, "HIGH").when(F.col("score") >= 11.3, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.max("score").over(w_grade_status).alias("grade_status_max_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status), F.lit(0)), 4).alias("score_share_of_grade_status"),
    )
)

course_id_stats = (
    windowed
    .groupBy("course_id")
    .agg(F.percentile_approx("grade_status_max_score", 0.5).alias("median_grade_status_max_score"))
)

above_average = (
    windowed.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.grade_status_max_score") >= F.col("g.median_grade_status_max_score"))
    .select(
        "s.*",
        "g.median_grade_status_max_score",
        F.round(F.col("s.grade_status_max_score") - F.col("g.median_grade_status_max_score"), 2).alias("grade_status_max_score_minus_median"),
    )
)

screened = (
    above_average
    .crossJoin(above_average.agg(F.avg("score_share_of_grade_status").alias("overall_avg_score_share_of_grade_status")))
    .filter(F.col("score_share_of_grade_status") > F.col("overall_avg_score_share_of_grade_status"))
    .drop("overall_avg_score_share_of_grade_status")
)

tagged = (
    screened
    .select(
        "id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_group",
        "score_tier",
        "grade_status_max_score",
        "score_share_of_grade_status",
        "median_grade_status_max_score",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

labeled = (
    tagged
    .filter(F.col("median_grade_status_max_score") < 800.0)
    .select(
        "id",
        "score",
        "term_group",
        "score_tier",
        F.greatest(F.col("grade_status_max_score"), F.col("score")).alias("greatest_grade_status_max_score_score"),
    )
)

screened_2 = (
    labeled
    .crossJoin(labeled.agg(F.avg("score").alias("overall_avg_score")))
    .filter(F.col("score") > F.col("overall_avg_score"))
    .drop("overall_avg_score")
)

result = (
    screened_2
    .select("id", "greatest_grade_status_max_score_score", "score")
    .unpivot(
        ids=["id"],
        values=["greatest_grade_status_max_score_score", "score"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
